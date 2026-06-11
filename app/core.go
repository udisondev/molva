// Package app — верхняя композиция molva: nodenet-узел плюс подсистемы
// протокола (конверты, надёжная доставка, крипто, хранилище). Все
// протокольные пакеты собираются здесь; только app, media и apptest
// импортируют nodenet.
package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/udisondev/molva/outbox"
	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/store"
	"github.com/udisondev/nodenet/identity"
	"github.com/udisondev/nodenet/node"
	"github.com/udisondev/nodenet/routing"
	"github.com/udisondev/nodenet/transport"
)

// Config описывает всё, что нужно ядру: идентичность, транспорт, каталог
// данных и параметры сети. Транспорт создаёт вызывающий (QUIC в проде,
// mem.Hub в тестах) — ядро не знает, поверх чего работает.
type Config struct {
	// Seed — master-seed идентичности; единственный долгоживущий секрет:
	// из него выводится и NodeID, и ключ шифрования истории.
	Seed [identity.SeedLen]byte
	// DataDir — каталог данных узла (БД); создаётся с правами 0700.
	DataDir string
	// Transport — транспорт nodenet; LocalID обязан совпадать с identity из Seed.
	Transport transport.Transport
	// Bootstrap — стартовые контакты сети.
	Bootstrap []routing.Contact
	// Dmin — PoW-сложность сети, к которой подключаемся (0 в тестах).
	Dmin int
	// Maintenance — параметры самоподдержки топологии; nil — дефолт nodenet.
	Maintenance *node.Maintenance
	// OnDelivery — отладочный перехват входящих payload'ов до разбора
	// конвертов; в проде не используется.
	OnDelivery func(from node.ID, payload []byte)
}

// Core — работающее ядро molva: nodenet-узел, хранилище, движок надёжной
// доставки и петля диспетчеризации входящих. Создаётся New, живёт в
// пределах Run; Run закрывает базу на выходе.
type Core struct {
	id     *identity.Identity
	node   *node.Node
	db     *store.DB
	outbox *outbox.Manager
	tap    func(from node.ID, payload []byte)
}

// New собирает ядро и открывает хранилище. Узел не запускается — это
// делает Run; если Run не будет вызван, базу надо закрыть самому (Close).
func New(cfg Config) (*Core, error) {
	id := identity.FromSeed(cfg.Seed)
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("app: каталог данных: %w", err)
	}
	db, err := store.Open(filepath.Join(cfg.DataDir, "molva.db"), store.KeyFromSeed(cfg.Seed))
	if err != nil {
		return nil, err
	}

	opts := []node.Option{node.WithDmin(cfg.Dmin)}
	if cfg.Maintenance != nil {
		opts = append(opts, node.WithMaintenance(*cfg.Maintenance))
	}
	n := node.New(id, cfg.Transport, opts...)
	n.Bootstrap(cfg.Bootstrap)

	c := &Core{
		id:   id,
		node: n,
		db:   db,
		tap:  cfg.OnDelivery,
	}
	c.outbox = outbox.NewManager(db, c.sendQueued, c.sendControl)
	return c, nil
}

// Run запускает узел, движок ретраев и петлю входящих; живёт до отмены ctx
// или закрытия транспорта. На выходе гасит подсистемы и закрывает базу.
func (c *Core) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Go(func() { _ = c.outbox.Run(ctx) })

	nodeErr := make(chan error, 1)
	go func() { nodeErr <- c.node.Run(ctx) }()

	for d := range c.node.Deliveries() {
		if c.tap != nil {
			c.tap(d.Originator, d.Payload)
		}
		c.outbox.HandleInbound(ctx, peer.ID(d.Originator), d.Payload)
	}

	err := <-nodeErr
	cancel()
	wg.Wait()
	if cerr := c.db.Close(); err == nil {
		err = cerr
	}
	return err
}

// Close освобождает ресурсы ядра, которое так и не запускали.
func (c *Core) Close() error { return c.db.Close() }

// sendQueued — путь элементов очереди: прямое ребро, при его отсутствии
// полный Connect-каскад внутри SendDirect. Ошибка оставляет элемент в
// очереди до следующего ретрая — статусы остаются честными (queued, пока
// кадр не ушёл адресату).
func (c *Core) sendQueued(ctx context.Context, to peer.ID, frame []byte) error {
	return c.node.SendDirect(ctx, node.ID(to), frame)
}

// sendControl — путь ack'ов: маршрутизируемая отправка, никогда не
// блокируется (зовётся с цикла доставки). При живом ребре до цели уйдёт
// по нему — ребро само ближайший хоп.
func (c *Core) sendControl(_ context.Context, to peer.ID, frame []byte) error {
	return c.node.Send(node.ID(to), frame)
}

// ID — собственный NodeID: сетевой адрес и публичный идентификатор.
func (c *Core) ID() node.ID { return c.id.ID() }

// Node отдаёт нижележащий nodenet-узел — для композиции верхних слоёв
// (прямые рёбра, медиа) и harness'а.
func (c *Core) Node() *node.Node { return c.node }

// Store — хранилище узла.
func (c *Core) Store() *store.DB { return c.db }

// Outbox — движок надёжной доставки.
func (c *Core) Outbox() *outbox.Manager { return c.outbox }
