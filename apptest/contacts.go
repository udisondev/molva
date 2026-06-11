package apptest

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/udisondev/molva/store"
)

// MakeContacts проводит полное знакомство узлов i и j (инвайт → запрос →
// принятие) и дожидается состояния «контакт» у обоих. Оба должны быть живы.
func MakeContacts(t *testing.T, c *Cluster, i, j int) {
	t.Helper()
	a, b := c.Node(i), c.Node(j)
	ctx := context.Background()

	invite := a.Core().Contacts().MyInvite("")
	if _, err := b.Core().Contacts().AddByInvite(ctx, invite); err != nil {
		t.Fatalf("добавление по инвайту: %v", err)
	}

	waitState(t, b, a, store.PeerPendingIn)
	if err := a.Core().Contacts().Accept(ctx, b.PeerID()); err != nil {
		t.Fatalf("принятие: %v", err)
	}
	waitState(t, b, a, store.PeerContact)
	waitState(t, a, b, store.PeerContact)
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
