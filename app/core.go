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
	"time"

	"github.com/udisondev/molva/blob"
	"github.com/udisondev/molva/callsig"
	"github.com/udisondev/molva/chat"
	"github.com/udisondev/molva/contact"
	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/group"
	"github.com/udisondev/molva/media"
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

	// События для слоя представления (IPC); все зовутся после коммита
	// соответствующей транзакции.
	OnMessage        func(store.Message)
	OnDelivered      func(peer.ID, envelope.MsgID)
	OnContactRequest func(peer.ID, string)
	OnContactAccept  func(peer.ID)
	OnPresence       func(peer.ID, bool)
	OnFileOffered    func(peer.ID, blob.Manifest)
	OnFileProgress   func(fileID [16]byte, have, total int)
	OnFileDone       func(fileID [16]byte, path string)
	OnGroupMessage   func(store.Message)
	OnCallIncoming   func(callsig.Call)
	OnCallState      func(callsig.Call)
	// OnMediaFrame — входящий медиакадр звонка; payload алиасит пул
	// транспорта, использовать строго синхронно.
	OnMediaFrame       func(ch uint8, rx time.Time, payload []byte)
	OnCallReconnecting func(callID [16]byte)
	// OnPreset — смена ступени лестницы качества видео.
	OnPreset func(media.Preset)
}

// Core — работающее ядро molva: nodenet-узел, хранилище, движок надёжной
// доставки и петля диспетчеризации входящих. Создаётся New, живёт в
// пределах Run; Run закрывает базу на выходе.
type Core struct {
	id       *identity.Identity
	node     *node.Node
	db       *store.DB
	outbox   *outbox.Manager
	contacts *contact.Manager
	chats    *chat.Manager
	blobs    *blob.Manager
	groups   *group.Manager
	calls    *callsig.Manager
	bridge   *media.Bridge
	onReconn func(callID [16]byte)
	tap      func(from node.ID, payload []byte)
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

	c0 := &Core{} // ранняя ссылка для consent-замыкания (заполняется ниже)
	opts := []node.Option{
		node.WithDmin(cfg.Dmin),
		node.WithMediaConsent(func(remote node.ID) bool {
			return c0.calls != nil && c0.calls.Consent(peer.ID(remote))
		}),
	}
	if cfg.Maintenance != nil {
		opts = append(opts, node.WithMaintenance(*cfg.Maintenance))
	}
	n := node.New(id, cfg.Transport, opts...)
	n.Bootstrap(cfg.Bootstrap)

	c := c0
	c.id, c.node, c.db, c.tap = id, n, db, cfg.OnDelivery
	c.outbox = outbox.NewManager(db, c.sendQueued, c.sendControl)
	c.outbox.SetOnDelivered(cfg.OnDelivered)

	self := peer.ID(id.ID())
	c.contacts, err = contact.NewManager(db, c.outbox, self, c.sendControl)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	c.contacts.SetCallbacks(cfg.OnContactRequest, cfg.OnContactAccept, cfg.OnPresence)
	c.outbox.SetGate(c.contacts.Gate)

	c.chats = chat.NewManager(db, c.outbox, cfg.Seed, self, func(p peer.ID) bool {
		return c.contacts.State(p) == store.PeerContact
	})
	c.chats.SetOnMessage(cfg.OnMessage)

	c.blobs, err = blob.NewManager(db, c.chats, filepath.Join(cfg.DataDir, "files"),
		c.sendQueued, c.sendControl, c.contacts.Online)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	c.blobs.SetCallbacks(cfg.OnFileOffered, cfg.OnFileProgress, cfg.OnFileDone)
	c.outbox.HandleFast(envelope.TypeFileChunk, c.blobs.HandleChunk)
	c.outbox.HandleFast(envelope.TypeFileChunkReq, c.blobs.HandleChunkReq)

	c.groups = group.NewManager(db, c.outbox, c.chats, cfg.Seed, self)
	c.groups.SetOnMessage(cfg.OnGroupMessage)

	c.bridge = media.NewBridge(cfg.OnMediaFrame, c.onMediaClosed)
	c.bridge.SetAdapter(media.NewAdapter(media.Preset720, cfg.OnPreset))
	c.calls = callsig.NewManager(c.chats, self)
	c.onReconn = cfg.OnCallReconnecting
	c.calls.SetCallbacks(cfg.OnCallIncoming, func(call callsig.Call) {
		if cfg.OnCallState != nil {
			cfg.OnCallState(call)
		}
		switch call.State {
		case callsig.StateActive:
			// Медиасессию открывает звонивший; ответившему она прилетит
			// в InboundMedia через consent-гейт.
			if call.Outgoing {
				go c.openMedia(call)
			}
		case callsig.StateEnded:
			c.bridge.Detach()
		}
	})
	return c, nil
}

// openMedia открывает медиапуть звонка (полный connect-каскад внутри).
func (c *Core) openMedia(call callsig.Call) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := c.node.OpenMedia(ctx, node.ID(call.Peer))
	if err != nil {
		return // путь не поднялся: пере-попытку даст следующий onMediaClosed/hangup
	}
	if cur, ok := c.calls.Current(); !ok || cur.ID != call.ID || cur.State != callsig.StateActive {
		_ = s.Close()
		return
	}
	c.bridge.Attach(s)
}

// onMediaClosed — смерть медиапути: make-before-break, повторный
// OpenMedia делает звонивший; звонок при этом живёт.
func (c *Core) onMediaClosed() {
	call, ok := c.calls.Current()
	if !ok || call.State != callsig.StateActive {
		return
	}
	if c.onReconn != nil {
		c.onReconn(call.ID)
	}
	if call.Outgoing {
		go c.openMedia(call)
	}
}

// Run запускает узел, движок ретраев и петлю входящих; живёт до отмены ctx
// или закрытия транспорта. На выходе гасит подсистемы и закрывает базу.
func (c *Core) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Go(func() { _ = c.outbox.Run(ctx) })
	wg.Go(func() { _ = c.contacts.RunPresence(ctx) })
	wg.Go(func() { _ = c.blobs.Run(ctx) })
	wg.Go(func() { _ = c.groups.Run(ctx) })

	nodeErr := make(chan error, 1)
	go func() { nodeErr <- c.node.Run(ctx) }()

	// Входящие медиасессии: consent-гейт nodenet уже пропустил только
	// активный звонок; сверяем с текущим и подключаем к мосту.
	wg.Go(func() {
		for s := range c.node.InboundMedia() {
			call, ok := c.calls.Current()
			if ok && call.State == callsig.StateActive && peer.ID(s.Remote()) == call.Peer {
				c.bridge.Attach(s)
			} else {
				_ = s.Close()
			}
		}
	})

	for d := range c.node.Deliveries() {
		if c.tap != nil {
			c.tap(d.Originator, d.Payload)
		}
		from := peer.ID(d.Originator)
		c.contacts.MarkActivity(from)
		c.outbox.HandleInbound(ctx, from, d.Payload)
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

// Contacts — круг общения: знакомство, блокировка, алиасы, presence.
func (c *Core) Contacts() *contact.Manager { return c.contacts }

// Chats — личные диалоги.
func (c *Core) Chats() *chat.Manager { return c.chats }

// Files — передача файлов.
func (c *Core) Files() *blob.Manager { return c.blobs }

// Groups — групповые чаты.
func (c *Core) Groups() *group.Manager { return c.groups }

// Calls — сигналинг звонков.
func (c *Core) Calls() *callsig.Manager { return c.calls }

// Media — медиамост активного звонка.
func (c *Core) Media() *media.Bridge { return c.bridge }
