// Package app — верхняя композиция molva: nodenet-узел плюс подсистемы
// протокола (конверты, надёжная доставка, крипто, хранилище). Все
// протокольные пакеты собираются здесь; только app, media и apptest
// импортируют nodenet.
package app

import (
	"context"

	"github.com/udisondev/nodenet/identity"
	"github.com/udisondev/nodenet/node"
	"github.com/udisondev/nodenet/routing"
	"github.com/udisondev/nodenet/transport"
)

// Config описывает всё, что нужно ядру: идентичность, транспорт и параметры
// сети. Транспорт создаёт вызывающий (QUIC в проде, mem.Hub в тестах) — ядро
// не знает, поверх чего работает.
type Config struct {
	// Seed — master-seed идентичности; единственный долгоживущий секрет.
	Seed [identity.SeedLen]byte
	// Transport — транспорт nodenet; LocalID обязан совпадать с identity из Seed.
	Transport transport.Transport
	// Bootstrap — стартовые контакты сети.
	Bootstrap []routing.Contact
	// Dmin — PoW-сложность сети, к которой подключаемся (0 в тестах).
	Dmin int
	// Maintenance — параметры самоподдержки топологии; nil — дефолт nodenet.
	Maintenance *node.Maintenance
	// OnDelivery — точка диспетчеризации входящих payload'ов. Сюда
	// подключается разбор конвертов; в тестах harness'а — прямой перехват.
	OnDelivery func(from node.ID, payload []byte)
}

// Core — работающее ядро molva: один nodenet-узел и петля диспетчеризации
// входящих. Создаётся New, живёт в пределах Run.
type Core struct {
	id         *identity.Identity
	node       *node.Node
	onDelivery func(from node.ID, payload []byte)
}

// New собирает ядро. Узел не запускается — это делает Run.
func New(cfg Config) *Core {
	id := identity.FromSeed(cfg.Seed)
	opts := []node.Option{node.WithDmin(cfg.Dmin)}
	if cfg.Maintenance != nil {
		opts = append(opts, node.WithMaintenance(*cfg.Maintenance))
	}
	n := node.New(id, cfg.Transport, opts...)
	n.Bootstrap(cfg.Bootstrap)
	return &Core{
		id:         id,
		node:       n,
		onDelivery: cfg.OnDelivery,
	}
}

// Run запускает узел и крутит петлю входящих, пока не отменён ctx или не
// закрыт транспорт. Возвращает ошибку узла.
func (c *Core) Run(ctx context.Context) error {
	errc := make(chan error, 1)
	go func() { errc <- c.node.Run(ctx) }()
	for d := range c.node.Deliveries() {
		if c.onDelivery != nil {
			c.onDelivery(d.Originator, d.Payload)
		}
	}
	return <-errc
}

// ID — собственный NodeID: сетевой адрес и публичный идентификатор.
func (c *Core) ID() node.ID { return c.id.ID() }

// Node отдаёт нижележащий nodenet-узел — для композиции верхних слоёв
// (прямые рёбра, медиа) и harness'а.
func (c *Core) Node() *node.Node { return c.node }
