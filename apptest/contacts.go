package apptest

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/udisondev/molva/store"
)

// MakeContacts добавляет узлы i и j друг другу по инвайтам. Одобрения
// знакомства нет — оба сразу контакты, сети для этого не нужно.
func MakeContacts(t *testing.T, c *Cluster, i, j int) {
	t.Helper()
	a, b := c.Node(i), c.Node(j)
	ctx := context.Background()

	if _, err := b.Core().Contacts().AddByInvite(ctx, a.Core().Contacts().MyInvite("")); err != nil {
		t.Fatalf("добавление по инвайту: %v", err)
	}
	if _, err := a.Core().Contacts().AddByInvite(ctx, b.Core().Contacts().MyInvite("")); err != nil {
		t.Fatalf("добавление по инвайту: %v", err)
	}
}

// waitState ждёт, пока узел on увидит пира who в состоянии want.
func waitState(t *testing.T, who, on *Node, want store.PeerState) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		synctest.Wait()
		if on.Core().Contacts().State(who.PeerID()) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node-%d не увидел node-%d в состоянии %d", on.index, who.index, want)
		}
		time.Sleep(time.Second)
	}
}
