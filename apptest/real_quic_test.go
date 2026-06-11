//go:build e2e_real

package apptest

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/udisondev/molva/app"
	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/store"
	"github.com/udisondev/nodenet/identity"
	"github.com/udisondev/nodenet/routing"
	"github.com/udisondev/nodenet/transport"
	quictr "github.com/udisondev/nodenet/transport/quic"
)

// Реальный QUIC на loopback: два полных ядра, знакомство, шифрованная
// переписка. Запуск: go test ./apptest -tags e2e_real -run TestRealQUIC -v
func TestRealQUICChat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	mk := func(seedByte byte, bootstrap []routing.Contact) (*app.Core, transport.Addr, func()) {
		t.Helper()
		seed := [identity.SeedLen]byte{seedByte}
		id := identity.FromSeed(seed)
		tr, err := quictr.Listen(id, "127.0.0.1:0")
		if err != nil {
			t.Fatalf("quic: %v", err)
		}
		core, err := app.New(app.Config{
			Seed:      seed,
			DataDir:   t.TempDir(),
			Transport: tr,
			Bootstrap: bootstrap,
		})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { defer close(done); _ = core.Run(ctx) }()
		stop := func() { _ = tr.Close(); <-done }
		return core, tr.LocalAddr(), stop
	}

	coreA, addrA, stopA := mk(1, nil)
	defer stopA()
	coreB, _, stopB := mk(2, []routing.Contact{{
		ID:    coreA.ID(),
		Addrs: []transport.Addr{addrA},
	}})
	defer stopB()

	peerA := peer.ID(coreA.ID())
	peerB := peer.ID(coreB.ID())

	// Знакомство по инвайту.
	invite := coreA.Contacts().MyInvite("Алиса")
	if _, err := coreB.Contacts().AddByInvite(ctx, invite); err != nil {
		t.Fatalf("AddByInvite: %v", err)
	}
	waitFor(t, ctx, "запрос знакомства у A", func() bool {
		return coreA.Contacts().State(peerB) == store.PeerPendingIn
	})
	if err := coreA.Contacts().Accept(ctx, peerB); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "контакт у B", func() bool {
		return coreB.Contacts().State(peerA) == store.PeerContact
	})

	// Шифрованная переписка в обе стороны.
	mid, err := coreB.Chats().SendText(ctx, peerA, "привет по настоящему QUIC")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "сообщение у A", func() bool {
		m, ok, _ := coreA.Store().GetMessage(ctx, peerB, mid, false)
		return ok && bytes.Equal(m.Body, []byte("привет по настоящему QUIC"))
	})
	mid2, err := coreA.Chats().SendText(ctx, peerB, "и тебе не хворать")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "ответ у B", func() bool {
		m, ok, _ := coreB.Store().GetMessage(ctx, peerA, mid2, false)
		return ok && m.Body != nil
	})
}

func waitFor(t *testing.T, ctx context.Context, what string, cond func() bool) {
	t.Helper()
	for {
		if cond() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("не дождались: %s", what)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
