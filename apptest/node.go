package apptest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/udisondev/molva/app"
	"github.com/udisondev/nodenet/identity"
	"github.com/udisondev/nodenet/node"
	"github.com/udisondev/nodenet/routing"
	"github.com/udisondev/nodenet/transport"
	"github.com/udisondev/nodenet/transport/mem"
)

// Delivery — перехваченный входящий payload уровня nodenet.
type Delivery struct {
	From    node.ID
	Payload []byte
}

// SetupFunc настраивает ядро между сборкой и запуском: регистрация
// обработчиков конвертов, колбэков. Вызывается и на каждом рестарте.
type SetupFunc func(i int, core *app.Core)

// Node — одно ядро molva в тестовом кластере: детерминированная identity,
// mem-транспорт, запущенный app.Core и перехват входящих. Переживает
// Kill/Restart с тем же seed'ом и каталогом данных.
type Node struct {
	t       *testing.T
	hub     *mem.Hub
	index   int
	seed    [identity.SeedLen]byte
	id      node.ID
	addr    transport.Addr
	dataDir string
	setup   SetupFunc

	inbox        chan Delivery
	inboxDropped atomic.Uint64

	mu     sync.Mutex
	alive  bool
	core   *app.Core
	tr     transport.Transport
	cancel context.CancelFunc
	done   chan error
}

func newNode(t *testing.T, hub *mem.Hub, index int, dataDir string, setup SetupFunc) *Node {
	seed := SeedFor(uint64(index) + 1)
	return &Node{
		t:       t,
		hub:     hub,
		index:   index,
		seed:    seed,
		id:      identity.FromSeed(seed).ID(),
		addr:    transport.Addr{Net: "mem", Endpoint: fmt.Sprintf("node-%d", index)},
		dataDir: dataDir,
		setup:   setup,
		inbox:   make(chan Delivery, 1024),
	}
}

// start поднимает ядро с актуальным bootstrap-списком. Падает тестом при
// ошибке транспорта или хранилища.
func (n *Node) start(contacts []routing.Contact) {
	n.t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.alive {
		n.t.Fatalf("node-%d: уже запущен", n.index)
	}
	tr, err := n.hub.New(n.id, n.addr)
	if err != nil {
		n.t.Fatalf("node-%d: транспорт: %v", n.index, err)
	}
	m := shortMaintenance()
	core, err := app.New(app.Config{
		Seed:        n.seed,
		DataDir:     n.dataDir,
		Transport:   tr,
		Bootstrap:   contacts,
		Maintenance: &m,
		OnDelivery: func(from node.ID, payload []byte) {
			select {
			case n.inbox <- Delivery{From: from, Payload: payload}:
			default:
				n.inboxDropped.Add(1)
			}
		},
	})
	if err != nil {
		n.t.Fatalf("node-%d: ядро: %v", n.index, err)
	}
	if n.setup != nil {
		n.setup(n.index, core)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- core.Run(ctx) }()

	n.tr, n.core, n.cancel, n.done = tr, core, cancel, done
	n.alive = true
}

// Kill обрывает узел жёстко: закрытие транспорта (рёбра умирают без
// прощаний), затем останов петли. Данные на диске остаются.
func (n *Node) Kill() {
	n.t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.alive {
		return
	}
	_ = n.tr.Close()
	n.cancel()
	<-n.done
	n.alive = false
}

// ID — постоянный NodeID узла (стабилен между рестартами: тот же seed).
func (n *Node) ID() node.ID { return n.id }

// Core — работающее ядро; nil после Kill.
func (n *Node) Core() *app.Core {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.alive {
		return nil
	}
	return n.core
}

// Alive — жив ли узел.
func (n *Node) Alive() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.alive
}

// DataDir — каталог данных узла; переживает Kill/Restart.
func (n *Node) DataDir() string { return n.dataDir }

// Inbox — перехваченные входящие payload'ы уровня nodenet.
func (n *Node) Inbox() <-chan Delivery { return n.inbox }

// InboxDropped — сколько входящих не влезло в перехват (тест обязан
// вычитывать вовремя; ненулевое значение — сигнал ошибки сценария).
func (n *Node) InboxDropped() uint64 { return n.inboxDropped.Load() }
