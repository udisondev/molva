// Package callsig — сигналинг звонков molva: offer/answer/reject/hangup
// едут надёжными конвертами по ratchet-сессиям, consent-гейт пускает
// медиасессию только при активном принятом звонке с этим пиром.
// Состояние звонков — в памяти (история звонков — не цель v1).
package callsig

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"sync"

	"github.com/udisondev/molva/chat"
	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/proto/callpb"
	"github.com/udisondev/molva/store"
	"google.golang.org/protobuf/proto"
)

// State — состояние звонка.
type State uint8

const (
	// StateRingingOut — мы позвонили, ждём ответа.
	StateRingingOut State = 1
	// StateRingingIn — нам звонят, ждём решения пользователя.
	StateRingingIn State = 2
	// StateActive — звонок принят; consent открыт для медиасессий.
	StateActive State = 3
	// StateEnded — завершён (hangup/reject/ошибка).
	StateEnded State = 4
)

// Ошибки сигналинга.
var (
	ErrBusy        = errors.New("callsig: уже есть звонок")
	ErrUnknownCall = errors.New("callsig: звонок неизвестен")
	ErrBadState    = errors.New("callsig: операция не подходит состоянию звонка")
	ErrMalformed   = errors.New("callsig: не разбирается")
)

// Call — снапшот звонка.
type Call struct {
	ID       [16]byte
	Peer     peer.ID
	State    State
	Outgoing bool
	Codecs   []string
}

// Manager — машина состояний звонков. Один звонок за раз (v1): второй
// входящий получает автоматический reject (busy).
type Manager struct {
	chats *chat.Manager
	self  peer.ID
	rnd   io.Reader

	mu      sync.Mutex
	current *Call

	onIncoming func(Call)
	onState    func(Call)
	ctr        counters
}

// NewManager регистрирует обработчики сигналинга.
func NewManager(chats *chat.Manager, self peer.ID) *Manager {
	m := &Manager{chats: chats, self: self, rnd: rand.Reader}
	chats.RegisterSealed(envelope.TypeCallOffer, m.onOffer)
	chats.RegisterSealed(envelope.TypeCallAnswer, m.onAnswer)
	chats.RegisterSealed(envelope.TypeCallHangup, m.onHangup)
	return m
}

// SetCallbacks — события для слоя представления (до запуска ядра).
func (m *Manager) SetCallbacks(onIncoming, onState func(Call)) {
	m.onIncoming = onIncoming
	m.onState = onState
}

// Current — текущий звонок, если есть.
func (m *Manager) Current() (Call, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil {
		return Call{}, false
	}
	return *m.current, true
}

// Consent — гейт входящих медиасессий nodenet: только при активном
// звонке с этим пиром. Зовётся с горутины медиагейта — быстрый и
// неблокирующий по построению.
func (m *Manager) Consent(remote peer.ID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	ok := m.current != nil && m.current.Peer == remote && m.current.State == StateActive
	if !ok {
		m.ctr.consentRefused.Add(1)
	}
	return ok
}

// Start начинает исходящий звонок контакту.
func (m *Manager) Start(ctx context.Context, to peer.ID, codecs []string) ([16]byte, error) {
	m.mu.Lock()
	if m.current != nil && m.current.State != StateEnded {
		m.mu.Unlock()
		return [16]byte{}, ErrBusy
	}
	m.mu.Unlock()

	var callID [16]byte
	if _, err := io.ReadFull(m.rnd, callID[:]); err != nil {
		return [16]byte{}, err
	}
	payload, err := proto.Marshal(&callpb.Offer{CallId: callID[:], Codecs: codecs})
	if err != nil {
		return [16]byte{}, err
	}
	if _, err := m.chats.SendSealed(ctx, to, envelope.TypeCallOffer, payload); err != nil {
		return [16]byte{}, err
	}
	call := &Call{ID: callID, Peer: to, State: StateRingingOut, Outgoing: true, Codecs: codecs}
	m.mu.Lock()
	m.current = call
	m.mu.Unlock()
	m.emitState(*call)
	return callID, nil
}

// Accept принимает входящий звонок: consent открывается, звонящий
// откроет медиасессию по нашему ответу.
func (m *Manager) Accept(ctx context.Context, callID [16]byte) error {
	m.mu.Lock()
	call := m.current
	if call == nil || call.ID != callID {
		m.mu.Unlock()
		return ErrUnknownCall
	}
	if call.State != StateRingingIn {
		m.mu.Unlock()
		return ErrBadState
	}
	call.State = StateActive
	snapshot := *call
	m.mu.Unlock()

	if err := m.sendAnswer(ctx, snapshot.Peer, callID, true); err != nil {
		return err
	}
	m.emitState(snapshot)
	return nil
}

// Reject отклоняет входящий звонок.
func (m *Manager) Reject(ctx context.Context, callID [16]byte) error {
	m.mu.Lock()
	call := m.current
	if call == nil || call.ID != callID {
		m.mu.Unlock()
		return ErrUnknownCall
	}
	if call.State != StateRingingIn {
		m.mu.Unlock()
		return ErrBadState
	}
	call.State = StateEnded
	snapshot := *call
	m.current = nil
	m.mu.Unlock()

	if err := m.sendAnswer(ctx, snapshot.Peer, callID, false); err != nil {
		return err
	}
	m.emitState(snapshot)
	return nil
}

// Hangup завершает звонок в любом состоянии.
func (m *Manager) Hangup(ctx context.Context, callID [16]byte) error {
	m.mu.Lock()
	call := m.current
	if call == nil || call.ID != callID {
		m.mu.Unlock()
		return ErrUnknownCall
	}
	call.State = StateEnded
	snapshot := *call
	m.current = nil
	m.mu.Unlock()

	payload, err := proto.Marshal(&callpb.Hangup{CallId: callID[:]})
	if err != nil {
		return err
	}
	if _, err := m.chats.SendSealed(ctx, snapshot.Peer, envelope.TypeCallHangup, payload); err != nil &&
		!errors.Is(err, chat.ErrNoSession) {
		return err
	}
	m.emitState(snapshot)
	return nil
}

func (m *Manager) sendAnswer(ctx context.Context, to peer.ID, callID [16]byte, accept bool) error {
	payload, err := proto.Marshal(&callpb.Answer{CallId: callID[:], Accept: accept})
	if err != nil {
		return err
	}
	_, err = m.chats.SendSealed(ctx, to, envelope.TypeCallAnswer, payload)
	return err
}

func (m *Manager) emitState(c Call) {
	if m.onState != nil {
		m.onState(c)
	}
}

// onOffer — входящий звонок.
func (m *Manager) onOffer(tx *store.Tx, from peer.ID, plain []byte) error {
	var pb callpb.Offer
	if err := proto.Unmarshal(plain, &pb); err != nil || len(pb.CallId) != 16 || len(pb.Codecs) > 8 {
		m.ctr.malformed.Add(1)
		return nil
	}
	var callID [16]byte
	copy(callID[:], pb.CallId)

	m.mu.Lock()
	busy := m.current != nil && m.current.State != StateEnded
	var snapshot Call
	if busy {
		// Коллизия встречных звонков: выживает звонок меньшего NodeID,
		// прочее busy.
		if m.current.Peer == from && m.current.State == StateRingingOut && less(from, m.self) {
			m.current = &Call{ID: callID, Peer: from, State: StateRingingIn, Codecs: pb.Codecs}
			snapshot = *m.current
			busy = false
		}
	} else {
		m.current = &Call{ID: callID, Peer: from, State: StateRingingIn, Codecs: pb.Codecs}
		snapshot = *m.current
	}
	m.mu.Unlock()

	if busy {
		m.ctr.busyRejects.Add(1)
		tx.AfterCommit(func() {
			_ = m.sendAnswer(context.Background(), from, callID, false)
		})
		return nil
	}
	tx.AfterCommit(func() {
		if m.onIncoming != nil {
			m.onIncoming(snapshot)
		}
	})
	return nil
}

// onAnswer — ответ на наш звонок.
func (m *Manager) onAnswer(tx *store.Tx, from peer.ID, plain []byte) error {
	var pb callpb.Answer
	if err := proto.Unmarshal(plain, &pb); err != nil || len(pb.CallId) != 16 {
		m.ctr.malformed.Add(1)
		return nil
	}
	var callID [16]byte
	copy(callID[:], pb.CallId)

	m.mu.Lock()
	call := m.current
	if call == nil || call.ID != callID || call.Peer != from || call.State != StateRingingOut {
		m.mu.Unlock()
		m.ctr.stale.Add(1)
		return nil
	}
	if pb.Accept {
		call.State = StateActive
	} else {
		call.State = StateEnded
		m.current = nil
	}
	snapshot := *call
	m.mu.Unlock()

	tx.AfterCommit(func() { m.emitState(snapshot) })
	return nil
}

// onHangup — пир повесил трубку.
func (m *Manager) onHangup(tx *store.Tx, from peer.ID, plain []byte) error {
	var pb callpb.Hangup
	if err := proto.Unmarshal(plain, &pb); err != nil || len(pb.CallId) != 16 {
		m.ctr.malformed.Add(1)
		return nil
	}
	var callID [16]byte
	copy(callID[:], pb.CallId)

	m.mu.Lock()
	call := m.current
	if call == nil || call.ID != callID || call.Peer != from {
		m.mu.Unlock()
		m.ctr.stale.Add(1)
		return nil
	}
	call.State = StateEnded
	snapshot := *call
	m.current = nil
	m.mu.Unlock()

	tx.AfterCommit(func() { m.emitState(snapshot) })
	return nil
}

// less — порядок NodeID для разрешения коллизий.
func less(a, b peer.ID) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}
