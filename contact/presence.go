package contact

import (
	"context"
	"crypto/rand"
	"sync"
	"time"

	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/outbox"
	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/store"
)

const (
	// probeInterval — период presence-зондов по контактам.
	probeInterval = 60 * time.Second
	// onlineTTL — сколько живёт признак «онлайн» без новых сигналов.
	onlineTTL = 90 * time.Second
)

// Presence — присутствие контактов: probe/pong поверх конвертов, мимо
// outbox и истории (молчание и есть ответ). Любой трафик пира — тоже
// признак онлайна и триггер немедленной отправки его очереди.
type Presence struct {
	self peer.ID
	send outbox.SendFunc
	mgr  *Manager

	onChange func(peer.ID, bool)

	mu       sync.Mutex
	lastSeen map[peer.ID]time.Time
	isOnline map[peer.ID]bool
}

func newPresence(self peer.ID, send outbox.SendFunc, mgr *Manager) *Presence {
	return &Presence{
		self:     self,
		send:     send,
		mgr:      mgr,
		lastSeen: make(map[peer.ID]time.Time),
		isOnline: make(map[peer.ID]bool),
	}
}

// run — probe-цикл: каждые probeInterval зонд каждому контакту и
// похороны протухших признаков онлайна.
func (pr *Presence) run(ctx context.Context) error {
	ticker := time.NewTicker(probeInterval)
	defer ticker.Stop()
	for {
		pr.probeAll(ctx)
		pr.sweep()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (pr *Presence) probeAll(ctx context.Context) {
	for _, p := range pr.mgr.contactIDs() {
		pr.sendSignal(ctx, p, envelope.TypeProbe)
	}
}

// sweep объявляет офлайн тех, кто замолчал дольше TTL.
func (pr *Presence) sweep() {
	now := time.Now()
	var gone []peer.ID
	pr.mu.Lock()
	for p, seen := range pr.lastSeen {
		if pr.isOnline[p] && now.Sub(seen) > onlineTTL {
			pr.isOnline[p] = false
			gone = append(gone, p)
		}
	}
	pr.mu.Unlock()
	if pr.onChange != nil {
		for _, p := range gone {
			pr.onChange(p, false)
		}
	}
}

// handleProbe — зонд пира: pong только взаимным контактам, незнакомцам и
// заблокированным статус не утекает (гейт отрезал их ещё раньше).
func (pr *Presence) handleProbe(from peer.ID, _ *envelope.Envelope) {
	if pr.mgr.State(from) != store.PeerContact {
		return
	}
	pr.sendSignal(context.Background(), from, envelope.TypePong)
	pr.markActivity(from)
}

// handlePong — контакт ответил: онлайн.
func (pr *Presence) handlePong(from peer.ID, _ *envelope.Envelope) {
	pr.markActivity(from)
}

// markActivity фиксирует признак жизни; переход офлайн→онлайн будит
// очередь к пиру (присутствие сбрасывает backoff).
func (pr *Presence) markActivity(p peer.ID) {
	if pr.mgr.State(p) != store.PeerContact {
		return
	}
	pr.mu.Lock()
	pr.lastSeen[p] = time.Now()
	wasOffline := !pr.isOnline[p]
	pr.isOnline[p] = true
	pr.mu.Unlock()
	if wasOffline {
		pr.mgr.ob.Flush(p)
		if pr.onChange != nil {
			pr.onChange(p, true)
		}
	}
}

// online — жив ли признак присутствия.
func (pr *Presence) online(p peer.ID) bool {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return pr.isOnline[p] && time.Since(pr.lastSeen[p]) <= onlineTTL
}

// forget стирает присутствие (блокировка).
func (pr *Presence) forget(p peer.ID) {
	pr.mu.Lock()
	delete(pr.lastSeen, p)
	delete(pr.isOnline, p)
	pr.mu.Unlock()
}

func (pr *Presence) sendSignal(ctx context.Context, to peer.ID, t envelope.Type) {
	mid, err := envelope.NewMsgID(rand.Reader)
	if err != nil {
		return
	}
	frame, err := envelope.Encode(envelope.Envelope{MsgID: mid, Type: t})
	if err != nil {
		return
	}
	_ = pr.send(ctx, to, frame) // молчание и есть ответ — ошибки не ретраим
}
