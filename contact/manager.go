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
	ErrUnknownPeer = errors.New("contact: пир неизвестен")
)

// Manager — круг общения узла: контакты, блокировка, локальные алиасы и
// гейт входящего трафика. Одобрения знакомства нет: добавил инвайт — пиши
// и звони сразу; входящее от незнакомца само появляется в эфире, защита —
// чёрный список. Авторитетное состояние — в store; в памяти живёт снапшот
// для быстрых проверок на цикле доставки (обновляется только после коммита).
type Manager struct {
	db   *store.DB
	ob   *outbox.Manager
	self peer.ID
	rnd  io.Reader

	mu     sync.RWMutex
	states map[peer.ID]store.PeerState

	presence *Presence

	onAdded func(peer.ID)
}

// NewManager загружает круг общения из store и регистрирует обработчики
// устаревших конвертов знакомства. Вызывать до Run движка доставки.
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

// SetCallbacks — события для верхнего слоя (до запуска ядра). onAdded
// зовётся после коммита, когда пир появился в эфире (инвайт или входящее).
func (m *Manager) SetCallbacks(onAdded func(peer.ID), onPresence func(peer.ID, bool)) {
	m.onAdded = onAdded
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

// contactIDs — снапшот контактов (для presence-обхода).
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

// Gate — фильтр входящих конвертов: заблокированный не получает ничего
// (дроп без ack — для него мы вечный офлайн), остальные проходят целиком.
// Первый содержательный конверт незнакомца сам добавит его в эфир
// (EnsureKnownTx с цикла доставки).
func (m *Manager) Gate(from peer.ID, _ envelope.Type) bool {
	return m.State(from) != store.PeerBlocked
}

// MyInvite — собственная инвайт-ссылка с предлагаемым алиасом.
func (m *Manager) MyInvite(alias string) string { return EncodeInvite(m.self, alias) }

// AddByInvite разбирает инвайт и сразу добавляет пира в контакты — писать
// и звонить можно немедленно, одобрение второй стороны не требуется.
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
	case store.PeerContact:
		return p, nil // уже в эфире
	}
	now := time.Now().UnixMilli()
	err = m.db.Tx(ctx, func(tx *store.Tx) error {
		if err := tx.PeerPut(p, store.PeerContact, now); err != nil {
			return err
		}
		if alias != "" {
			if err := tx.PeerAliasSet(p, alias, now); err != nil {
				return err
			}
		}
		tx.AfterCommit(func() {
			m.setState(p, store.PeerContact)
			if m.onAdded != nil {
				m.onAdded(p)
			}
		})
		return nil
	})
	if err != nil {
		return peer.ID{}, err
	}
	return p, nil
}

// EnsureKnownTx добавляет пира в эфир при первом содержательном конверте
// от него — внутри транзакции обработки этого конверта. Состояние читается
// из store, а не из снапшота: блокировка, закоммиченная параллельной
// транзакцией, не должна перетереться.
func (m *Manager) EnsureKnownTx(tx *store.Tx, from peer.ID, suggested string) error {
	if from == m.self {
		return nil
	}
	info, exists, err := tx.PeerGet(from)
	if err != nil {
		return err
	}
	if exists && info.State != store.PeerNone {
		return nil
	}
	now := time.Now().UnixMilli()
	if err := tx.PeerPut(from, store.PeerContact, now); err != nil {
		return err
	}
	if suggested != "" {
		if err := tx.PeerAliasSet(from, suggested, now); err != nil {
			return err
		}
	}
	tx.AfterCommit(func() {
		m.setState(from, store.PeerContact)
		m.ob.Flush(from)
		if m.onAdded != nil {
			m.onAdded(from)
		}
	})
	return nil
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

// enqueueSignal ставит служебный конверт в надёжную очередь.
func (m *Manager) enqueueSignal(tx *store.Tx, to peer.ID, t envelope.Type, payload []byte) error {
	mid, err := envelope.NewMsgID(m.rnd)
	if err != nil {
		return err
	}
	return m.ob.EnqueueTx(tx, to, envelope.Envelope{MsgID: mid, Type: t, Payload: payload})
}

// onContactRequest — запрос знакомства от узла старой сборки: одобрение
// упразднено, пир просто добавляется в эфир. Ответный accept отпускает
// его гейт ожидания (новые сборки этот конверт игнорируют молча).
func (m *Manager) onContactRequest(tx *store.Tx, from peer.ID, env *envelope.Envelope) error {
	if err := m.EnsureKnownTx(tx, from, clampAlias(string(env.Payload))); err != nil {
		return err
	}
	if err := m.enqueueSignal(tx, from, envelope.TypeContactAccept, nil); err != nil {
		return err
	}
	tx.AfterCommit(func() { m.ob.Flush(from) })
	return nil
}

// onContactAccept — accept от узла старой сборки: знакомство и так не
// требует одобрения, достаточно убедиться, что пир в эфире.
func (m *Manager) onContactAccept(tx *store.Tx, from peer.ID, _ *envelope.Envelope) error {
	return m.EnsureKnownTx(tx, from, "")
}
