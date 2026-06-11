package apptest

import (
	"fmt"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/udisondev/nodenet/routing"
	"github.com/udisondev/nodenet/transport"
	"github.com/udisondev/nodenet/transport/mem"
)

// Cluster — кластер molva-ядер на одном mem.Hub. Создаётся внутри
// synctest.Test; всё время — фейковое, сходимость топологии занимает
// секунды виртуальных часов и нули реальных.
type Cluster struct {
	t     *testing.T
	hub   *mem.Hub
	nodes []*Node
}

// Option настраивает кластер при создании.
type Option func(*clusterConfig)

type clusterConfig struct {
	setup SetupFunc
}

// WithSetup задаёт настройку каждого ядра между сборкой и запуском
// (регистрация обработчиков); вызывается и на рестартах.
func WithSetup(f SetupFunc) Option {
	return func(cfg *clusterConfig) { cfg.setup = f }
}

// NewCluster поднимает n ядер, связывает полной сеткой и дожидается её
// сходимости. По завершении теста живые узлы гасятся автоматически.
func NewCluster(t *testing.T, n int, opts ...Option) *Cluster {
	t.Helper()
	var cfg clusterConfig
	for _, o := range opts {
		o(&cfg)
	}
	c := &Cluster{
		t:   t,
		hub: mem.NewHub(mem.WithInboundBuffer(64)),
	}
	base := t.TempDir()
	for i := range n {
		c.nodes = append(c.nodes, newNode(t, c.hub, i, filepath.Join(base, fmt.Sprintf("node-%d", i)), cfg.setup))
	}
	for i := range n {
		c.nodes[i].start(c.contacts(i))
	}
	t.Cleanup(c.shutdown)
	c.WaitMesh()
	return c
}

// Node — узел по индексу.
func (c *Cluster) Node(i int) *Node { return c.nodes[i] }

// Len — размер кластера (включая убитые узлы).
func (c *Cluster) Len() int { return len(c.nodes) }

// Kill жёстко обрывает узел i.
func (c *Cluster) Kill(i int) { c.nodes[i].Kill() }

// Restart поднимает убитый узел i с тем же seed'ом и каталогом данных,
// с bootstrap'ом на живых соседей, и дожидается сходимости сетки.
func (c *Cluster) Restart(i int) {
	c.t.Helper()
	c.nodes[i].start(c.contacts(i))
	c.WaitMesh()
}

// Partition разрывает связь между узлами i и j в обе стороны.
func (c *Cluster) Partition(i, j int) {
	c.hub.Partition(c.nodes[i].ID(), c.nodes[j].ID())
}

// Heal восстанавливает связь между узлами i и j.
func (c *Cluster) Heal(i, j int) {
	c.hub.Heal(c.nodes[i].ID(), c.nodes[j].ID())
}

// SetLinkProfile задаёт модель медиалинка в направлении i→j (потери,
// джиттер, шейпер) — только для медиадатаграмм.
func (c *Cluster) SetLinkProfile(i, j int, p mem.LinkProfile) {
	c.hub.SetLinkProfile(c.nodes[i].ID(), c.nodes[j].ID(), p)
}

// WaitMesh ждёт, пока каждый живой узел получит живое ребро до каждого
// другого живого узла. Падает тестом, если сетка не сошлась за разумное
// виртуальное время.
func (c *Cluster) WaitMesh() {
	c.t.Helper()
	const timeout = 2 * time.Minute
	deadline := time.Now().Add(timeout)
	for {
		synctest.Wait()
		if c.meshComplete() {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("сетка не сошлась за %v", timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (c *Cluster) meshComplete() bool {
	alive := c.aliveNodes()
	for _, a := range alive {
		edges := a.Core().Node().Edges()
		for _, b := range alive {
			if a == b {
				continue
			}
			if _, ok := edges.Conn(b.ID()); !ok {
				return false
			}
		}
	}
	return true
}

func (c *Cluster) aliveNodes() []*Node {
	var out []*Node
	for _, n := range c.nodes {
		if n.Alive() {
			out = append(out, n)
		}
	}
	return out
}

// contacts — bootstrap-список для узла i: все остальные узлы кластера.
// Мёртвые включаются тоже: знание о них безвредно, maintenance отсеет.
func (c *Cluster) contacts(i int) []routing.Contact {
	var out []routing.Contact
	for j, n := range c.nodes {
		if j == i {
			continue
		}
		out = append(out, routing.Contact{
			ID:    n.ID(),
			Addrs: []transport.Addr{n.addr},
		})
	}
	return out
}

func (c *Cluster) shutdown() {
	for _, n := range c.nodes {
		n.Kill()
		if d := n.InboxDropped(); d != 0 {
			c.t.Errorf("node-%d: перехват входящих дропнул %d — сценарий не вычитывал вовремя", n.index, d)
		}
	}
}
