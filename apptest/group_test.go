package apptest

import (
	"bytes"
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/udisondev/molva/store"
)

// waitGroupKnown ждёт, пока узел узнает группу заданной версии.
func waitGroupKnown(t *testing.T, n *Node, gid [32]byte, minVersion uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Minute)
	for {
		synctest.Wait()
		groups, err := n.Core().Groups().Groups(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, g := range groups {
			if g.GroupID == gid && g.Version >= minVersion && !g.Left {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("node-%d не узнал группу v%d", n.index, minVersion)
		}
		time.Sleep(2 * time.Second)
	}
}

// waitGroupMessage ждёт групповое сообщение с телом text у узла n.
func waitGroupMessage(t *testing.T, n *Node, gid [32]byte, text string) store.Message {
	t.Helper()
	deadline := time.Now().Add(15 * time.Minute)
	for {
		synctest.Wait()
		msgs, err := n.Core().Groups().Messages(context.Background(), gid, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			if bytes.Equal(m.Body, []byte(text)) {
				return m
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("node-%d не получил %q; stats=%+v", n.index, text, n.Core().Groups().Stats())
		}
		time.Sleep(2 * time.Second)
	}
}

// makeMesh заводит контакты и сессии каждый-с-каждым (UI это форсит).
func makeMesh(t *testing.T, c *Cluster, idx ...int) {
	t.Helper()
	for i := 0; i < len(idx); i++ {
		for j := i + 1; j < len(idx); j++ {
			MakeContacts(t, c, idx[i], idx[j])
			a, b := c.Node(idx[i]), c.Node(idx[j])
			mid := sendText(t, a, b.PeerID(), "рукопожатие")
			WaitInboundMessage(t, b, a.PeerID(), mid, 5*time.Minute)
		}
	}
}

// Полный групповой цикл: создание, приглашения, веерная переписка;
// новичок не читает историю до вступления; бывший не читает новое
// после удаления (обязательный rekey).
func TestGroupLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 3)
		admin, b, late := c.Node(0), c.Node(1), c.Node(2)
		ctx := context.Background()
		makeMesh(t, c, 0, 1, 2)

		gid, err := admin.Core().Groups().Create(ctx, "радиокружок")
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := admin.Core().Groups().Add(ctx, gid, b.PeerID()); err != nil {
			t.Fatalf("Add(b): %v", err)
		}
		waitGroupKnown(t, b, gid, 2)

		// Переписка в обе стороны.
		if _, err := admin.Core().Groups().SendText(ctx, gid, "история до новичка"); err != nil {
			t.Fatalf("SendText: %v", err)
		}
		waitGroupMessage(t, b, gid, "история до новичка")
		if _, err := b.Core().Groups().SendText(ctx, gid, "ответ участника"); err != nil {
			t.Fatalf("SendText(b): %v", err)
		}
		waitGroupMessage(t, admin, gid, "ответ участника")

		// Новичок: видит новое, не видит историю.
		if err := admin.Core().Groups().Add(ctx, gid, late.PeerID()); err != nil {
			t.Fatalf("Add(late): %v", err)
		}
		waitGroupKnown(t, late, gid, 3)
		if _, err := admin.Core().Groups().SendText(ctx, gid, "при новичке"); err != nil {
			t.Fatal(err)
		}
		waitGroupMessage(t, late, gid, "при новичке")
		waitGroupMessage(t, b, gid, "при новичке")

		msgs, _ := late.Core().Groups().Messages(ctx, gid, 0)
		for _, m := range msgs {
			if bytes.Equal(m.Body, []byte("история до новичка")) || bytes.Equal(m.Body, []byte("ответ участника")) {
				t.Fatal("новичок прочитал историю до вступления")
			}
		}

		// Сообщение новичка доходит всем (его ключ разъехался).
		if _, err := late.Core().Groups().SendText(ctx, gid, "я в эфире"); err != nil {
			t.Fatal(err)
		}
		waitGroupMessage(t, admin, gid, "я в эфире")
		waitGroupMessage(t, b, gid, "я в эфире")

		// Удаление: бывший не читает новое.
		if err := admin.Core().Groups().Remove(ctx, gid, b.PeerID()); err != nil {
			t.Fatalf("Remove: %v", err)
		}
		// Подождать, пока membership и rekey разъедутся оставшимся.
		deadline := time.Now().Add(10 * time.Minute)
		for {
			synctest.Wait()
			groups, _ := late.Core().Groups().Groups(ctx)
			ok := false
			for _, g := range groups {
				if g.GroupID == gid && g.Version >= 4 {
					ok = true
				}
			}
			if ok {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("обновление членства не разъехалось")
			}
			time.Sleep(2 * time.Second)
		}

		before := len(listBodies(t, b, gid))
		if _, err := admin.Core().Groups().SendText(ctx, gid, "без бывших"); err != nil {
			t.Fatal(err)
		}
		waitGroupMessage(t, late, gid, "без бывших")

		// Бывший за это время ничего нового не получил и помечен left.
		time.Sleep(5 * time.Minute)
		synctest.Wait()
		if got := len(listBodies(t, b, gid)); got != before {
			t.Fatalf("бывший получил новое: %d -> %d", before, got)
		}
		groupsB, _ := b.Core().Groups().Groups(ctx)
		for _, g := range groupsB {
			if g.GroupID == gid && !g.Left {
				t.Fatal("бывший не помечен left")
			}
		}
	})
}

func listBodies(t *testing.T, n *Node, gid [32]byte) []store.Message {
	t.Helper()
	msgs, err := n.Core().Groups().Messages(context.Background(), gid, 0)
	if err != nil {
		t.Fatal(err)
	}
	return msgs
}

// Гонка «сообщение раньше ключа»: ретраи доставляют после приезда ключа,
// ровно один раз.
func TestGroupMessageBeforeKey(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 2)
		admin, b := c.Node(0), c.Node(1)
		ctx := context.Background()
		makeMesh(t, c, 0, 1)

		gid, err := admin.Core().Groups().Create(ctx, "гонка")
		if err != nil {
			t.Fatal(err)
		}
		if err := admin.Core().Groups().Add(ctx, gid, b.PeerID()); err != nil {
			t.Fatal(err)
		}
		waitGroupKnown(t, b, gid, 2)

		// b отправляет мгновенно после вступления: его ключ мог не доехать
		// до admin — ретраи обязаны дотащить ровно один раз.
		if _, err := b.Core().Groups().SendText(ctx, gid, "сразу после входа"); err != nil {
			t.Fatal(err)
		}
		waitGroupMessage(t, admin, gid, "сразу после входа")
		msgs, _ := admin.Core().Groups().Messages(ctx, gid, 0)
		count := 0
		for _, m := range msgs {
			if bytes.Equal(m.Body, []byte("сразу после входа")) {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("сообщение продублировалось: %d", count)
		}
	})
}

