// Package chat — личные диалоги molva: композиция ratchet + envelope +
// outbox. Отправка шифрует Double Ratchet'ом и едет через персистентную
// очередь; приём расшифровывает и пишет историю — всё в одной транзакции
// с дедупом и крипто-состоянием. Сессии устанавливаются интерактивно при
// первой отправке; тексты до сессии ждут в очереди ожидания.
package chat

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"errors"
	"io"
	"time"

	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/outbox"
	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/ratchet"
	"github.com/udisondev/molva/store"
)

// MaxTextLen — потолок текста сообщения (plaintext, байты UTF-8).
const MaxTextLen = 32 << 10

// Ошибки отправки.
var (
	ErrPeerBlocked = errors.New("chat: пир в чёрном списке")
	ErrEmptyText   = errors.New("chat: пустое сообщение")
	ErrTooLong     = errors.New("chat: сообщение длиннее потолка")
	// ErrNoSession — сессии нет или она не готова к отправке; рукопожатие
	// уже запущено, операцию надо повторить после его завершения.
	ErrNoSession = errors.New("chat: сессия устанавливается — повторите позже")
)

// SealedHandler обрабатывает расшифрованный payload служебного конверта
// (манифесты файлов, групповые ключи) внутри общей транзакции приёма.
type SealedHandler func(tx *store.Tx, from peer.ID, plaintext []byte) error

// Manager — движок личных диалогов. Регистрация обработчиков происходит
// в New; колбэк OnMessage — до запуска ядра.
type Manager struct {
	db      *store.DB
	ob      *outbox.Manager
	ik      *ecdh.PrivateKey
	self    peer.ID
	rnd     io.Reader
	canSend func(peer.ID) bool

	onMessage func(store.Message)
	ctr       counters
}

// NewManager выводит identity-ключ ratchet-слоя из master-seed и
// регистрирует обработчики диалоговых конвертов. canSend — гейт исходящих
// (false для чёрного списка: очередь к заблокированному не растёт).
func NewManager(db *store.DB, ob *outbox.Manager, seed [32]byte, self peer.ID, canSend func(peer.ID) bool) *Manager {
	m := &Manager{
		db:      db,
		ob:      ob,
		ik:      ratchet.IdentityFromSeed(seed),
		self:    self,
		rnd:     rand.Reader,
		canSend: canSend,
	}
	ob.Handle(envelope.TypeChat, m.onChat)
	ob.Handle(envelope.TypeSessionInit, m.onSessionInit)
	ob.Handle(envelope.TypeSessionInitAck, m.onSessionInitAck)
	return m
}

// SetOnMessage — колбэк принятого сообщения (после коммита).
func (m *Manager) SetOnMessage(f func(store.Message)) { m.onMessage = f }

// RegisterSealed — обработчик расшифрованных payload'ов конвертов типа t
// (до запуска ядра). Доставка надёжная (outbox/дедуп), шифрование — DR.
func (m *Manager) RegisterSealed(t envelope.Type, h SealedHandler) {
	m.ob.Handle(t, func(tx *store.Tx, from peer.ID, env *envelope.Envelope) error {
		plain, st, ok, err := m.decryptIncoming(tx, from, env)
		if err != nil || !ok {
			return err
		}
		if err := h(tx, from, plain); err != nil {
			return err
		}
		if err := m.drainPending(tx, from, st); err != nil {
			return err
		}
		return m.saveSession(tx, from, st)
	})
}

// SendSealed шифрует произвольный payload сессией DR и ставит конверт
// типа t в надёжную очередь (история не пишется). Без готовой сессии
// запускает рукопожатие и возвращает ErrNoSession.
func (m *Manager) SendSealed(ctx context.Context, to peer.ID, t envelope.Type, plaintext []byte) (envelope.MsgID, error) {
	if !m.canSend(to) {
		return envelope.MsgID{}, ErrPeerBlocked
	}
	mid, err := envelope.NewMsgID(m.rnd)
	if err != nil {
		return envelope.MsgID{}, err
	}
	noSession := false
	err = m.db.Tx(ctx, func(tx *store.Tx) error {
		st, ok, err := m.loadSession(tx, to)
		if err != nil {
			return err
		}
		if !ok || !st.CanSend() {
			noSession = true
			return m.ensureHandshake(tx, to)
		}
		rm, err := st.Encrypt(plaintext)
		if err != nil {
			return err
		}
		payload, err := ratchet.EncodeMessage(rm)
		if err != nil {
			return err
		}
		if err := m.ob.EnqueueTx(tx, to, envelope.Envelope{MsgID: mid, Type: t, Payload: payload}); err != nil {
			return err
		}
		return m.saveSession(tx, to, st)
	})
	if err != nil {
		return envelope.MsgID{}, err
	}
	m.ob.Flush(to)
	if noSession {
		return envelope.MsgID{}, ErrNoSession
	}
	return mid, nil
}

// SessionReady — установлена ли отправная сессия с пиром.
func (m *Manager) SessionReady(ctx context.Context, p peer.ID) (bool, error) {
	ready := false
	err := m.db.Tx(ctx, func(tx *store.Tx) error {
		st, ok, err := m.loadSession(tx, p)
		if err != nil {
			return err
		}
		ready = ok && st.CanSend()
		return nil
	})
	return ready, err
}

// SendText ставит текст в доставку: история и очередь — одной
// транзакцией. Без установленной сессии текст ждёт рукопожатия.
func (m *Manager) SendText(ctx context.Context, to peer.ID, text string) (envelope.MsgID, error) {
	if !m.canSend(to) {
		return envelope.MsgID{}, ErrPeerBlocked
	}
	if text == "" {
		return envelope.MsgID{}, ErrEmptyText
	}
	if len(text) > MaxTextLen {
		return envelope.MsgID{}, ErrTooLong
	}
	mid, err := envelope.NewMsgID(m.rnd)
	if err != nil {
		return envelope.MsgID{}, err
	}
	now := time.Now().UnixMilli()
	err = m.db.Tx(ctx, func(tx *store.Tx) error {
		seq, err := tx.NextSeq("seq:" + to.String())
		if err != nil {
			return err
		}
		lam, err := tx.LamportNext()
		if err != nil {
			return err
		}
		if _, err := tx.InsertMessage(&store.Message{
			Peer: to, MsgID: mid, Outgoing: true, FromSeq: seq, Lamport: lam,
			SentAt: now, Status: store.StatusQueued, Body: []byte(text),
		}); err != nil {
			return err
		}

		st, ok, err := m.loadSession(tx, to)
		if err != nil {
			return err
		}
		if !ok || !st.CanSend() {
			// Сессии нет (или мы респондент без отправной цепочки —
			// хотим писать первыми, значит сами инициируем рукопожатие).
			if err := tx.PendingChatAdd(to, mid, now); err != nil {
				return err
			}
			return m.ensureHandshake(tx, to)
		}
		if err := m.encryptEnqueue(tx, st, to, mid, seq, lam, []byte(text)); err != nil {
			return err
		}
		return m.saveSession(tx, to, st)
	})
	if err != nil {
		return envelope.MsgID{}, err
	}
	m.ob.Flush(to)
	return mid, nil
}

// Delete — локальное удаление контента сообщения.
func (m *Manager) Delete(ctx context.Context, p peer.ID, mid envelope.MsgID) error {
	return m.db.Tx(ctx, func(tx *store.Tx) error { return tx.DeleteMessageBody(p, mid) })
}

// Messages — история диалога в порядке отображения.
func (m *Manager) Messages(ctx context.Context, p peer.ID, limit int) ([]store.Message, error) {
	return m.db.ListMessages(ctx, p, limit)
}

// decryptIncoming — общий путь приёма DR-шифртекста: разбор, загрузка
// сессии, расшифровка. ok=false — конверт съеден (мусор или рассинхрон,
// лечение уже запущено), обрабатывать нечего.
func (m *Manager) decryptIncoming(tx *store.Tx, from peer.ID, env *envelope.Envelope) (plain []byte, st *ratchet.State, ok bool, err error) {
	rm, err := ratchet.DecodeMessage(env.Payload)
	if err != nil {
		m.ctr.malformed.Add(1)
		return nil, nil, false, nil // мусор от аутентифицированного пира: съесть
	}
	st, found, err := m.loadSession(tx, from)
	if err != nil {
		return nil, nil, false, err
	}
	if !found {
		// Пир думает, что сессия есть, у нас её нет: съесть и восстановить
		// связь свежим рукопожатием; конкретно это сообщение потеряно.
		m.ctr.noSession.Add(1)
		return nil, nil, false, m.ensureHandshake(tx, from)
	}
	plain, err = st.Decrypt(m.rnd, rm)
	if err != nil {
		// Состояния разошлись: то же лечение. Объект st выброшен.
		m.ctr.decryptFailures.Add(1)
		return nil, nil, false, m.ensureHandshake(tx, from)
	}
	return plain, st, true, nil
}

// onChat — входящее сообщение диалога.
func (m *Manager) onChat(tx *store.Tx, from peer.ID, env *envelope.Envelope) error {
	plain, st, ok, err := m.decryptIncoming(tx, from, env)
	if err != nil || !ok {
		return err
	}
	if err := tx.LamportObserve(env.LamportTS); err != nil {
		return err
	}
	// Детект дыр per-sender: пропуск более ранних сообщений виден в счётчике
	// (надёжность даёт outbox, переотправку не запрашиваем).
	if gap, err := tx.RecvSeqObserve("rseq:"+from.String(), env.FromSeq); err != nil {
		return err
	} else if gap {
		m.ctr.seqGaps.Add(1)
	}
	msg := store.Message{
		Peer: from, MsgID: env.MsgID, Outgoing: false, FromSeq: env.FromSeq,
		Lamport: store.ClampLamport(env.LamportTS), SentAt: time.Now().UnixMilli(),
		Status: store.StatusDelivered, Body: plain,
	}
	if _, err := tx.InsertMessage(&msg); err != nil {
		return err
	}
	// Первое принятое открывает отправную цепочку респондента — пора
	// отправить тексты, ждавшие сессию.
	if err := m.drainPending(tx, from, st); err != nil {
		return err
	}
	if err := m.saveSession(tx, from, st); err != nil {
		return err
	}
	tx.AfterCommit(func() {
		if m.onMessage != nil {
			m.onMessage(msg)
		}
	})
	return nil
}

// onSessionInit — пир инициирует сессию.
func (m *Manager) onSessionInit(tx *store.Tx, from peer.ID, env *envelope.Envelope) error {
	init, err := ratchet.DecodeInit(env.Payload)
	if err != nil {
		m.ctr.malformed.Add(1)
		return nil
	}
	if _, ours, err := tx.HandshakeGet(from); err != nil {
		return err
	} else if ours && less(m.self, from) {
		// Коллизия рукопожатий: инициатором остаётся меньший NodeID — мы.
		// Его init молча гаснет (наш он примет и ответит).
		m.ctr.collisionsIgnored.Add(1)
		return nil
	}
	st, ack, err := ratchet.AcceptHandshake(m.rnd, m.ik, init, from, m.self)
	if err != nil {
		m.ctr.malformed.Add(1)
		return nil
	}
	// Наше проигравшее рукопожатие (если было) умирает; его сессия
	// замещает старую (re-handshake пира — штатное лечение рассинхрона).
	if err := tx.HandshakeDelete(from); err != nil {
		return err
	}
	payload, err := ratchet.EncodeInitAck(ack)
	if err != nil {
		return err
	}
	mid, err := envelope.NewMsgID(m.rnd)
	if err != nil {
		return err
	}
	if err := m.ob.EnqueueTx(tx, from, envelope.Envelope{
		MsgID: mid, Type: envelope.TypeSessionInitAck, Payload: payload,
	}); err != nil {
		return err
	}
	if err := m.saveSession(tx, from, st); err != nil {
		return err
	}
	m.ctr.sessionsAccepted.Add(1)
	// Респондент не имеет отправной цепочки до первого принятого сообщения
	// (канон DR). Тексты, ждущие отправки, сливаются сами, как только от
	// пира приходит первое сообщение (drainPending в onChat/RegisterSealed):
	// при коллизии победитель пишет первым, и его сообщение открывает нашу
	// цепочку. Спекулятивная переинициация здесь гонилась бы с уже летящими
	// конвертами и теряла бы их — поэтому не делаем.
	tx.AfterCommit(func() { m.ob.Flush(from) })
	return nil
}

// onSessionInitAck — респондент ответил на наше рукопожатие.
func (m *Manager) onSessionInitAck(tx *store.Tx, from peer.ID, env *envelope.Envelope) error {
	ack, err := ratchet.DecodeInitAck(env.Payload)
	if err != nil {
		m.ctr.malformed.Add(1)
		return nil
	}
	blob, ok, err := tx.HandshakeGet(from)
	if err != nil {
		return err
	}
	if !ok {
		// Ответ на рукопожатие, которого больше нет (проиграло коллизию
		// или уже завершено) — штатный мусор.
		m.ctr.staleAcks.Add(1)
		return nil
	}
	hs, err := ratchet.UnmarshalHandshake(blob, m.ik)
	if err != nil {
		return err
	}
	st, err := hs.Finish(m.rnd, ack, m.self, from)
	if err != nil {
		m.ctr.staleAcks.Add(1)
		return nil
	}
	if err := tx.HandshakeDelete(from); err != nil {
		return err
	}
	if err := m.drainPending(tx, from, st); err != nil {
		return err
	}
	if err := m.saveSession(tx, from, st); err != nil {
		return err
	}
	m.ctr.sessionsEstablished.Add(1)
	tx.AfterCommit(func() { m.ob.Flush(from) })
	return nil
}

// drainPending шифрует и ставит в очередь тексты, ждавшие сессию.
// Состояние st продвигается, сохранение — на вызывающем.
func (m *Manager) drainPending(tx *store.Tx, to peer.ID, st *ratchet.State) error {
	if !st.CanSend() {
		return nil
	}
	mids, err := tx.PendingChatTake(to)
	if err != nil {
		return err
	}
	for _, mid := range mids {
		msg, ok, err := tx.GetMessage(to, mid, true)
		if err != nil {
			return err
		}
		if !ok || msg.Deleted || msg.Body == nil {
			continue // удалено, пока ждало — не отправляем
		}
		if err := m.encryptEnqueue(tx, st, to, mid, msg.FromSeq, msg.Lamport, msg.Body); err != nil {
			return err
		}
		m.ctr.pendingDrained.Add(1)
	}
	return nil
}

// encryptEnqueue шифрует текст очередным шагом сессии и ставит конверт в
// надёжную очередь.
func (m *Manager) encryptEnqueue(tx *store.Tx, st *ratchet.State, to peer.ID, mid envelope.MsgID, seq, lam uint64, body []byte) error {
	rm, err := st.Encrypt(body)
	if err != nil {
		return err
	}
	payload, err := ratchet.EncodeMessage(rm)
	if err != nil {
		return err
	}
	return m.ob.EnqueueTx(tx, to, envelope.Envelope{
		MsgID: mid, Type: envelope.TypeChat, FromSeq: seq, LamportTS: lam, Payload: payload,
	})
}

// ensureHandshake начинает рукопожатие, если оно ещё не идёт.
func (m *Manager) ensureHandshake(tx *store.Tx, to peer.ID) error {
	if _, ok, err := tx.HandshakeGet(to); err != nil {
		return err
	} else if ok {
		return nil
	}
	hs, err := ratchet.NewHandshake(m.rnd, m.ik)
	if err != nil {
		return err
	}
	sid := hs.SID()
	if err := tx.HandshakePut(to, hs.Marshal(), sid[:], time.Now().UnixMilli()); err != nil {
		return err
	}
	payload, err := ratchet.EncodeInit(hs.Init())
	if err != nil {
		return err
	}
	mid, err := envelope.NewMsgID(m.rnd)
	if err != nil {
		return err
	}
	if err := m.ob.EnqueueTx(tx, to, envelope.Envelope{
		MsgID: mid, Type: envelope.TypeSessionInit, Payload: payload,
	}); err != nil {
		return err
	}
	m.ctr.sessionsInitiated.Add(1)
	return nil
}

func (m *Manager) loadSession(tx *store.Tx, p peer.ID) (*ratchet.State, bool, error) {
	raw, ok, err := tx.SessionGet(p)
	if err != nil || !ok {
		return nil, false, err
	}
	st, err := ratchet.Unmarshal(raw)
	if err != nil {
		return nil, false, err
	}
	return st, true, nil
}

func (m *Manager) saveSession(tx *store.Tx, p peer.ID, st *ratchet.State) error {
	raw, err := st.Marshal()
	if err != nil {
		return err
	}
	return tx.SessionPut(p, raw, time.Now().UnixMilli())
}

// less — порядок NodeID для разрешения коллизий (меньший — инициатор).
func less(a, b peer.ID) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}
