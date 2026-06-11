package contact

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/outbox"
	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/store"
)

// Ошибки операций с контактами.
var (
	ErrSelf        = errors.New("contact: это твой собственный идентификатор")
	ErrBlocked     = errors.New("contact: пир заблокирован")
	ErrNotPending  = errors.New("contact: нет входящего запроса от этого пира")
	ErrUnknownPeer = errors.New("contact: пир неизвестен")
)

// Manager — круг общения узла: знакомство (запрос/принятие/отказ),
// блокировка, локальные алиасы и гейт входящего трафика. Авторитетное
// состояние — в store; в памяти живёт снапшот для быстрых проверок на
// цикле доставки (обновляется только после коммита).
type Manager struct {
	db   *store.DB
	ob   *outbox.Manager
	self peer.ID
	rnd  io.Reader

	mu     sync.RWMutex
	states map[peer.ID]store.PeerState

	presence *Presence

	onRequest  func(peer.ID, string)
	onAccepted func(peer.ID)
}

// NewManager загружает круг общения из store и регистрирует обработчики
// знакомства. Вызывать до Run движка доставки.
func NewManager(db *store.DB, ob *outbox.Manager, self peer.ID, sendControl outbox.SendFunc) (*Manager, error) {
	m := &Manager{
		db:     db,
		ob:     ob,
		self:   self,
		rnd:    rand.Reader,
		states: make(map[peer.ID]store.PeerState),
	}
	infos, err := db.PeerList(context.Background())
	if err != nil {
		return nil, err
	}
	for _, info := range infos {
		m.states[info.Peer] = info.State
	}
	m.presence = newPresence(self, sendControl, m)
	ob.Handle(envelope.TypeContactRequest, m.onContactRequest)
	ob.Handle(envelope.TypeContactAccept, m.onContactAccept)
	ob.HandleFast(envelope.TypeProbe, m.presence.handleProbe)
	ob.HandleFast(envelope.TypePong, m.presence.handlePong)
	return m, nil
}

// SetCallbacks — события для верхнего слоя (до запуска ядра).
func (m *Manager) SetCallbacks(onRequest func(peer.ID, string), onAccepted func(peer.ID), onPresence func(peer.ID, bool)) {
	m.onRequest = onRequest
	m.onAccepted = onAccepted
	m.presence.onChange = onPresence
}

// State — текущее состояние пира из снапшота.
func (m *Manager) State(p peer.ID) store.PeerState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.states[p]
}

func (m *Manager) setState(p peer.ID, s store.PeerState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s == store.PeerNone {
		delete(m.states, p)
		return
	}
	m.states[p] = s
}

// contactIDs — снапшот принятых контактов (для presence-обхода).
func (m *Manager) contactIDs() []peer.ID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]peer.ID, 0, len(m.states))
	for p, s := range m.states {
		if s == store.PeerContact {
			out = append(out, p)
		}
	}
	return out
}

// Gate — фильтр входящих конвертов: блок не получает ничего, незнакомец —
// только запрос знакомства; полный трафик — после принятия.
func (m *Manager) Gate(from peer.ID, t envelope.Type) bool {
	switch m.State(from) {
	case store.PeerBlocked:
		return false
	case store.PeerContact:
		return true
	case store.PeerPendingOut:
		// Мы запросили знакомство: ждём ack нашего запроса, его accept
		// или встречный запрос.
		return t == envelope.TypeAck || t == envelope.TypeContactAccept || t == envelope.TypeContactRequest
	case store.PeerPendingIn:
		return t == envelope.TypeAck || t == envelope.TypeContactRequest
	default: // незнакомец
		return t == envelope.TypeContactRequest
	}
}

// MyInvite — собственная инвайт-ссылка с предлагаемым алиасом.
func (m *Manager) MyInvite(alias string) string { return EncodeInvite(m.self, alias) }

// AddByInvite разбирает инвайт и отправляет запрос знакомства. Если пир
// уже сам стучался к нам — это взаимность: принимаем сразу.
func (m *Manager) AddByInvite(ctx context.Context, invite string) (peer.ID, error) {
	p, alias, err := ParseInvite(invite)
	if err != nil {
		return peer.ID{}, err
	}
	if p == m.self {
		return peer.ID{}, ErrSelf
	}
	switch m.State(p) {
	case store.PeerBlocked:
		return peer.ID{}, ErrBlocked
	case store.PeerContact, store.PeerPendingOut:
		return p, nil // уже в работе
	case store.PeerPendingIn:
		return p, m.Accept(ctx, p)
	}
	now := time.Now().UnixMilli()
	err = m.db.Tx(ctx, func(tx *store.Tx) error {
		if err := tx.PeerPut(p, store.PeerPendingOut, now); err != nil {
			return err
		}
		if alias != "" {
			if err := tx.PeerAliasSet(p, alias, now); err != nil {
				return err
			}
		}
		if err := m.enqueueSignal(tx, p, envelope.TypeContactRequest, nil); err != nil {
			return err
		}
		tx.AfterCommit(func() {
			m.setState(p, store.PeerPendingOut)
			m.ob.Flush(p)
		})
		return nil
	})
	if err != nil {
		return peer.ID{}, err
	}
	return p, nil
}

// Accept принимает входящий запрос знакомства.
func (m *Manager) Accept(ctx context.Context, p peer.ID) error {
	if m.State(p) != store.PeerPendingIn {
		return ErrNotPending
	}
	now := time.Now().UnixMilli()
	return m.db.Tx(ctx, func(tx *store.Tx) error {
		if err := tx.PeerPut(p, store.PeerContact, now); err != nil {
			return err
		}
		if err := m.enqueueSignal(tx, p, envelope.TypeContactAccept, nil); err != nil {
			return err
		}
		tx.AfterCommit(func() {
			m.setState(p, store.PeerContact)
			m.ob.Flush(p)
		})
		return nil
	})
}

// Reject отклоняет входящий запрос: запись стирается, пир ничего не
// узнаёт (для него это неотличимо от офлайна).
func (m *Manager) Reject(ctx context.Context, p peer.ID) error {
	if m.State(p) != store.PeerPendingIn {
		return ErrNotPending
	}
	return m.db.Tx(ctx, func(tx *store.Tx) error {
		if err := tx.PeerDelete(p); err != nil {
			return err
		}
		tx.AfterCommit(func() { m.setState(p, store.PeerNone) })
		return nil
	})
}

// Block — терминальное состояние: весь трафик пира дропается без ack,
// очередь к нему очищается, крипто-сессия стирается. Пир не уведомляется.
func (m *Manager) Block(ctx context.Context, p peer.ID) error {
	if p == m.self {
		return ErrSelf
	}
	now := time.Now().UnixMilli()
	return m.db.Tx(ctx, func(tx *store.Tx) error {
		if err := tx.PeerPut(p, store.PeerBlocked, now); err != nil {
			return err
		}
		if err := tx.OutboxPurgePeer(p); err != nil {
			return err
		}
		if err := tx.SealedOutboxPurgePeer(p); err != nil {
			return err
		}
		if _, err := tx.PendingChatTake(p); err != nil {
			return err
		}
		if err := tx.SessionDelete(p); err != nil {
			return err
		}
		if err := tx.HandshakeDelete(p); err != nil {
			return err
		}
		tx.AfterCommit(func() {
			m.setState(p, store.PeerBlocked)
			m.presence.forget(p)
		})
		return nil
	})
}

// Unblock возвращает пира в незнакомцы.
func (m *Manager) Unblock(ctx context.Context, p peer.ID) error {
	if m.State(p) != store.PeerBlocked {
		return ErrUnknownPeer
	}
	return m.db.Tx(ctx, func(tx *store.Tx) error {
		if err := tx.PeerDelete(p); err != nil {
			return err
		}
		tx.AfterCommit(func() { m.setState(p, store.PeerNone) })
		return nil
	})
}

// SetAlias задаёт локальный алиас (хранится шифрованно).
func (m *Manager) SetAlias(ctx context.Context, p peer.ID, alias string) error {
	alias = clampAlias(alias)
	return m.db.Tx(ctx, func(tx *store.Tx) error {
		return tx.PeerAliasSet(p, alias, time.Now().UnixMilli())
	})
}

// Contacts — все записи круга общения (для UI).
func (m *Manager) Contacts(ctx context.Context) ([]store.PeerInfo, error) {
	return m.db.PeerList(ctx)
}

// Online — статус присутствия контакта.
func (m *Manager) Online(p peer.ID) bool { return m.presence.online(p) }

// MarkActivity — любой трафик пира как признак присутствия; зовётся с
// цикла доставки ядра.
func (m *Manager) MarkActivity(p peer.ID) { m.presence.markActivity(p) }

// RunPresence крутит probe-цикл присутствия до отмены ctx.
func (m *Manager) RunPresence(ctx context.Context) error { return m.presence.run(ctx) }

// enqueueSignal ставит служебный конверт знакомства в надёжную очередь.
func (m *Manager) enqueueSignal(tx *store.Tx, to peer.ID, t envelope.Type, payload []byte) error {
	mid, err := envelope.NewMsgID(m.rnd)
	if err != nil {
		return err
	}
	return m.ob.EnqueueTx(tx, to, envelope.Envelope{MsgID: mid, Type: t, Payload: payload})
}

// onContactRequest — входящий запрос знакомства.
func (m *Manager) onContactRequest(tx *store.Tx, from peer.ID, env *envelope.Envelope) error {
	suggested := clampAlias(string(env.Payload))
	now := time.Now().UnixMilli()
	info, exists, err := tx.PeerGet(from)
	if err != nil {
		return err
	}
	state := store.PeerNone
	if exists {
		state = info.State
	}
	switch state {
	case store.PeerNone:
		if err := tx.PeerPut(from, store.PeerPendingIn, now); err != nil {
			return err
		}
		if suggested != "" {
			if err := tx.PeerAliasSet(from, suggested, now); err != nil {
				return err
			}
		}
		tx.AfterCommit(func() {
			m.setState(from, store.PeerPendingIn)
			if m.onRequest != nil {
				m.onRequest(from, suggested)
			}
		})
	case store.PeerPendingOut:
		// Взаимные запросы: обе стороны хотели — знакомство состоялось.
		if err := tx.PeerPut(from, store.PeerContact, now); err != nil {
			return err
		}
		if err := m.enqueueSignal(tx, from, envelope.TypeContactAccept, nil); err != nil {
			return err
		}
		tx.AfterCommit(func() {
			m.setState(from, store.PeerContact)
			m.ob.Flush(from)
			if m.onAccepted != nil {
				m.onAccepted(from)
			}
		})
	case store.PeerContact:
		// Пир потерял наш accept (или своё состояние) — повторим.
		if err := m.enqueueSignal(tx, from, envelope.TypeContactAccept, nil); err != nil {
			return err
		}
		tx.AfterCommit(func() { m.ob.Flush(from) })
	case store.PeerPendingIn:
		// Повторный запрос — идемпотентно.
	}
	return nil
}

// onContactAccept — наш запрос принят.
func (m *Manager) onContactAccept(tx *store.Tx, from peer.ID, env *envelope.Envelope) error {
	info, exists, err := tx.PeerGet(from)
	if err != nil {
		return err
	}
	if !exists || info.State != store.PeerPendingOut {
		// Гейт пропускает accept только для pending_out; здесь — повтор.
		return nil
	}
	if err := tx.PeerPut(from, store.PeerContact, time.Now().UnixMilli()); err != nil {
		return err
	}
	tx.AfterCommit(func() {
		m.setState(from, store.PeerContact)
		m.ob.Flush(from)
		if m.onAccepted != nil {
			m.onAccepted(from)
		}
	})
	return nil
}
