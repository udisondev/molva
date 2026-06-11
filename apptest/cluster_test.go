package apptest

import (
	"bytes"
	"context"
	"testing"
	"testing/synctest"
	"time"
)

// waitDelivery ждёт входящий payload на узле до дедлайна фейковых часов.
func waitDelivery(t *testing.T, n *Node) Delivery {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		synctest.Wait()
		select {
		case d := <-n.Inbox():
			return d
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("node-%d: доставка не пришла", n.index)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Кластер сходится в полную сетку, маршрутизируемая и прямая отправка
// доставляют payload с верным отправителем.
func TestClusterMeshAndDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 3)
		a, b := c.Node(0), c.Node(2)

		if err := a.Core().Node().Send(b.ID(), []byte("привет, оверлей")); err != nil {
			t.Fatalf("Send: %v", err)
		}
		got := waitDelivery(t, b)
		if got.From != a.ID() || !bytes.Equal(got.Payload, []byte("привет, оверлей")) {
			t.Fatalf("доставлено не то: from=%x payload=%q", got.From[:4], got.Payload)
		}

		if err := a.Core().Node().SendDirect(context.Background(), b.ID(), []byte("привет, ребро")); err != nil {
			t.Fatalf("SendDirect: %v", err)
		}
		got = waitDelivery(t, b)
		if got.From != a.ID() || !bytes.Equal(got.Payload, []byte("привет, ребро")) {
			t.Fatalf("доставлено не то: from=%x payload=%q", got.From[:4], got.Payload)
		}
	})
}

// Узел убит — оставшиеся продолжают общаться; рестарт с тем же seed'ом
// возвращает тот же NodeID, и сетка срастается обратно.
func TestClusterKillRestartSameSeed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 3)
		victim := c.Node(1)
		idBefore := victim.ID()

		c.Kill(1)
		if victim.Alive() {
			t.Fatal("узел должен быть мёртв")
		}

		// Выжившие общаются как ни в чём не бывало.
		if err := c.Node(0).Core().Node().Send(c.Node(2).ID(), []byte("живы")); err != nil {
			t.Fatalf("Send между живыми: %v", err)
		}
		got := waitDelivery(t, c.Node(2))
		if !bytes.Equal(got.Payload, []byte("живы")) {
			t.Fatalf("доставлено не то: %q", got.Payload)
		}

		c.Restart(1)
		if victim.ID() != idBefore {
			t.Fatal("NodeID после рестарта с тем же seed обязан совпадать")
		}

		// Вернувшийся снова достижим.
		if err := c.Node(0).Core().Node().Send(victim.ID(), []byte("с возвращением")); err != nil {
			t.Fatalf("Send вернувшемуся: %v", err)
		}
		got = waitDelivery(t, victim)
		if got.From != c.Node(0).ID() || !bytes.Equal(got.Payload, []byte("с возвращением")) {
			t.Fatalf("доставлено не то: from=%x payload=%q", got.From[:4], got.Payload)
		}
	})
}

// Прямая связь двух узлов разорвана — после того как keepalive похоронит
// мёртвое ребро, маршрутизируемая отправка едет в обход через третий узел.
// (Пока заблэкхоленное ребро числится живым, копии Send честно гибнут —
// best-effort nodenet; надёжность поверх строит outbox molva.)
func TestClusterPartitionRoutedDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 3)
		c.Partition(0, 1)

		edges := c.Node(0).Core().Node().Edges()
		deadline := time.Now().Add(time.Minute)
		for {
			synctest.Wait()
			if _, ok := edges.Conn(c.Node(1).ID()); !ok {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("мёртвое ребро не похоронено keepalive'ом")
			}
			time.Sleep(500 * time.Millisecond)
		}

		if err := c.Node(0).Core().Node().Send(c.Node(1).ID(), []byte("в обход")); err != nil {
			t.Fatalf("Send: %v", err)
		}
		got := waitDelivery(t, c.Node(1))
		if got.From != c.Node(0).ID() || !bytes.Equal(got.Payload, []byte("в обход")) {
			t.Fatalf("доставлено не то: from=%x payload=%q", got.From[:4], got.Payload)
		}

		c.Heal(0, 1)
	})
}
