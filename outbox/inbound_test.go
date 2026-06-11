package outbox

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/store"
)

var (
	peerA = peer.ID{0xA1}
	peerB = peer.ID{0xB2}
)

// sentLog — потокобезопасный накопитель кадров, ушедших через SendFunc.
type sentLog struct {
	mu     sync.Mutex
	frames [][]byte
	fail   bool
}

func (l *sentLog) send(_ context.Context, _ peer.ID, frame []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fail {
		return errors.New("сеть лежит")
	}
	l.frames = append(l.frames, bytes.Clone(frame))
	return nil
}

func (l *sentLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.frames)
}

func (l *sentLog) last() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.frames) == 0 {
		return nil
	}
	return l.frames[len(l.frames)-1]
}

func (l *sentLog) setFail(v bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.fail = v
}

func openDB(t *testing.T, name string) (*store.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	d, err := store.Open(path, store.KeyFromSeed([32]byte{0xDB}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d, path
}

func chatFrame(t *testing.T, mid envelope.MsgID, text string) []byte {
	t.Helper()
	frame, err := envelope.Encode(envelope.Envelope{
		MsgID: mid, Type: envelope.TypeChat, FromSeq: 1, LamportTS: 1, Payload: []byte(text),
	})
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

// Мусор и кривые ack'и не паникуют и видны в счётчике.
func TestInboundMalformed(t *testing.T) {
	db, _ := openDB(t, "m.db")
	ctrl := &sentLog{}
	m := NewManager(db, ctrl.send, ctrl.send)
	ctx := context.Background()

	m.HandleInbound(ctx, peerA, []byte{0xff, 0xfe, 0xfd})
	if got := m.Stats().InboundMalformed; got != 1 {
		t.Fatalf("InboundMalformed = %d, want 1", got)
	}

	// Ack с payload неверной длины.
	bad, _ := envelope.Encode(envelope.Envelope{
		MsgID: envelope.MsgID{1}, Type: envelope.TypeAck, Payload: []byte{1, 2, 3},
	})
	m.HandleInbound(ctx, peerA, bad)
	if got := m.Stats().InboundMalformed; got != 2 {
		t.Fatalf("InboundMalformed = %d, want 2", got)
	}
	if ctrl.count() != 0 {
		t.Fatal("на мусор ничего не отвечаем")
	}
}

// Надёжный тип без обработчика: не ack'ается (мы не обработали — пусть
// ретраит до появления обработчика), виден в счётчике.
func TestInboundUnhandled(t *testing.T) {
	db, _ := openDB(t, "m.db")
	ctrl := &sentLog{}
	m := NewManager(db, ctrl.send, ctrl.send)

	m.HandleInbound(context.Background(), peerA, chatFrame(t, envelope.MsgID{7}, "x"))
	if got := m.Stats().Unhandled; got != 1 {
		t.Fatalf("Unhandled = %d, want 1", got)
	}
	if ctrl.count() != 0 {
		t.Fatal("ack на необработанное уходить не должен")
	}
}

// Fast-типы идут мимо дедупа и ack'ов.
func TestInboundFast(t *testing.T) {
	db, _ := openDB(t, "m.db")
	ctrl := &sentLog{}
	m := NewManager(db, ctrl.send, ctrl.send)
	var calls int
	m.HandleFast(envelope.TypeProbe, func(from peer.ID, env *envelope.Envelope) { calls++ })

	probe, _ := envelope.Encode(envelope.Envelope{MsgID: envelope.MsgID{9}, Type: envelope.TypeProbe})
	ctx := context.Background()
	m.HandleInbound(ctx, peerA, probe)
	m.HandleInbound(ctx, peerA, probe) // тот же msg_id — дедупа нет
	if calls != 2 {
		t.Fatalf("fast-обработчик звался %d раз, want 2", calls)
	}
	if ctrl.count() != 0 {
		t.Fatal("fast не ack'ается")
	}
	if seen, _ := db.DedupSeen(ctx, peerA, envelope.MsgID{9}); seen {
		t.Fatal("fast не должен попадать в окно дедупа")
	}
}

// Потерянный ack: пере-доставка гасится дедупом и пере-ack'ается; ack
// рассчитывает очередь отправителя ровно один раз. Дедуп переживает
// рестарт получателя.
func TestLostAckReAckAndSettle(t *testing.T) {
	ctx := context.Background()
	mid := envelope.MsgID{0x42}
	frame := chatFrame(t, mid, "письмо")

	// Получатель B: первый ack теряется.
	dbB, pathB := openDB(t, "b.db")
	ctrlB := &sentLog{}
	mB := NewManager(dbB, ctrlB.send, ctrlB.send)
	registerRecorder := func(m *Manager) {
		m.Handle(envelope.TypeChat, func(tx *store.Tx, from peer.ID, env *envelope.Envelope) error {
			_, err := tx.InsertMessage(&store.Message{
				Peer: from, MsgID: env.MsgID, Outgoing: false, SentAt: 1,
				Status: store.StatusDelivered, Body: env.Payload,
			})
			return err
		})
	}
	registerRecorder(mB)

	ctrlB.setFail(true)
	mB.HandleInbound(ctx, peerA, frame)
	st := mB.Stats()
	if st.AckSendFailures != 1 || st.AcksSent != 0 {
		t.Fatalf("потерянный ack: %+v", st)
	}
	if msgs, _ := dbB.ListMessages(ctx, peerA, 0); len(msgs) != 1 {
		t.Fatalf("история: %d, want 1", len(msgs))
	}

	// Рестарт получателя: дедуп персистентен.
	if err := dbB.Close(); err != nil {
		t.Fatal(err)
	}
	dbB2, err := store.Open(pathB, store.KeyFromSeed([32]byte{0xDB}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dbB2.Close() })
	ctrlB2 := &sentLog{}
	mB2 := NewManager(dbB2, ctrlB2.send, ctrlB2.send)
	registerRecorder(mB2)

	// Пере-доставка после рестарта: дедуп-хит, пере-ack уходит.
	mB2.HandleInbound(ctx, peerA, frame)
	st = mB2.Stats()
	if st.DedupHits != 1 || st.AcksSent != 1 {
		t.Fatalf("пере-доставка: %+v", st)
	}
	if msgs, _ := dbB2.ListMessages(ctx, peerA, 0); len(msgs) != 1 {
		t.Fatalf("история после пере-доставки: %d, want 1", len(msgs))
	}

	// Отправитель A: ack рассчитывает очередь и поднимает статус.
	dbA, _ := openDB(t, "a.db")
	ctrlA := &sentLog{}
	mA := NewManager(dbA, ctrlA.send, ctrlA.send)
	err = dbA.Tx(ctx, func(tx *store.Tx) error {
		if _, err := tx.InsertMessage(&store.Message{
			Peer: peerB, MsgID: mid, Outgoing: true, SentAt: 1,
			Status: store.StatusSent, Body: []byte("письмо"),
		}); err != nil {
			return err
		}
		return mA.EnqueueTx(tx, peerB, envelope.Envelope{
			MsgID: mid, Type: envelope.TypeChat, FromSeq: 1, LamportTS: 1, Payload: []byte("письмо"),
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	mA.HandleInbound(ctx, peerB, ctrlB2.last())
	if n, _ := dbA.OutboxPending(ctx, peerB); n != 0 {
		t.Fatalf("очередь после ack: %d, want 0", n)
	}
	msg, ok, _ := dbA.GetMessage(ctx, peerB, mid, true)
	if !ok || msg.Status != store.StatusDelivered {
		t.Fatalf("статус: %+v ok=%v", msg, ok)
	}
	if got := mA.Stats().Delivered; got != 1 {
		t.Fatalf("Delivered = %d, want 1", got)
	}
	// Повторный ack — unknown, без эффекта.
	mA.HandleInbound(ctx, peerB, ctrlB2.last())
	if got := mA.Stats().AcksUnknown; got != 1 {
		t.Fatalf("AcksUnknown = %d, want 1", got)
	}
}

// Пере-доставка после вытеснения дедуп-окна не дублирует историю и всё
// равно ack'ается: идемпотентность InsertMessage страхует второй линией.
func TestRedeliveryAfterDedupPrune(t *testing.T) {
	ctx := context.Background()
	db, _ := openDB(t, "b.db")
	ctrl := &sentLog{}
	m := NewManager(db, ctrl.send, ctrl.send)
	m.Handle(envelope.TypeChat, func(tx *store.Tx, from peer.ID, env *envelope.Envelope) error {
		_, err := tx.InsertMessage(&store.Message{
			Peer: from, MsgID: env.MsgID, Outgoing: false, SentAt: 1,
			Status: store.StatusDelivered, Body: env.Payload,
		})
		return err
	})

	mid := envelope.MsgID{0x55}
	frame := chatFrame(t, mid, "до подрезки")
	m.HandleInbound(ctx, peerA, frame)

	// Окно дедупа вытеснено (возраст).
	err := db.Tx(ctx, func(tx *store.Tx) error {
		return tx.DedupPrune(peerA, time.Now().UnixMilli()+1000, 100)
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen, _ := db.DedupSeen(ctx, peerA, mid); seen {
		t.Fatal("подрезка не сработала")
	}

	m.HandleInbound(ctx, peerA, frame)
	if msgs, _ := db.ListMessages(ctx, peerA, 0); len(msgs) != 1 {
		t.Fatalf("история продублировалась: %d", len(msgs))
	}
	if got := m.Stats().AcksSent; got != 2 {
		t.Fatalf("AcksSent = %d, want 2 (пере-доставка обязана ack'аться)", got)
	}
}

// Гейт дропает без ack'а и без обработки.
func TestInboundGate(t *testing.T) {
	db, _ := openDB(t, "m.db")
	ctrl := &sentLog{}
	m := NewManager(db, ctrl.send, ctrl.send)
	m.Handle(envelope.TypeChat, func(tx *store.Tx, from peer.ID, env *envelope.Envelope) error {
		t.Fatal("обработчик не должен зваться за гейтом")
		return nil
	})
	m.SetGate(func(from peer.ID, tp envelope.Type) bool { return false })

	m.HandleInbound(context.Background(), peerA, chatFrame(t, envelope.MsgID{1}, "мимо"))
	if got := m.Stats().GateDropped; got != 1 {
		t.Fatalf("GateDropped = %d, want 1", got)
	}
	if ctrl.count() != 0 {
		t.Fatal("дроп гейта не ack'ается")
	}
}

var _ = rand.Reader // силуэт для будущих тестов с rand
