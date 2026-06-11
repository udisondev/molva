package apptest

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/udisondev/molva/app"
	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/store"
)

// Получатель офлайн → сообщение ждёт в outbox → получатель вернулся →
// доставлено ровно один раз, ack рассчитал очередь.
func TestDeliveryOfflineToOnlineExactlyOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var handled atomic.Int64
		c := NewCluster(t, 2, WithSetup(func(i int, core *app.Core) {
			if i == 1 {
				RecordChat(core, func() error { handled.Add(1); return nil })
			}
		}))
		a, b := c.Node(0), c.Node(1)
		ctx := context.Background()

		c.Kill(1)
		mid := SendChat(t, a, b.PeerID(), "догонишь — прочтёшь")

		// Пока B мёртв: попытки горят, статус честно queued, очередь не пуста.
		time.Sleep(30 * time.Second)
		synctest.Wait()
		m, ok, err := a.Core().Store().GetMessage(ctx, b.PeerID(), mid, true)
		if err != nil || !ok {
			t.Fatalf("исходящее пропало: %v %v", ok, err)
		}
		if m.Status != store.StatusQueued {
			t.Fatalf("статус при офлайне = %v, want queued", m.Status)
		}
		if n, _ := a.Core().Store().OutboxPending(ctx, b.PeerID()); n != 1 {
			t.Fatalf("очередь к B: %d, want 1", n)
		}

		c.Restart(1)
		WaitMessageStatus(t, a, b.PeerID(), mid, store.StatusDelivered, 15*time.Minute)

		got := WaitInboundMessage(t, b, a.PeerID(), mid, time.Minute)
		if !bytes.Equal(got.Body, []byte("догонишь — прочтёшь")) {
			t.Fatalf("тело: %q", got.Body)
		}
		if n, _ := a.Core().Store().OutboxPending(ctx, b.PeerID()); n != 0 {
			t.Fatalf("очередь после ack: %d, want 0", n)
		}

		// Ровно один раз: ждём ещё и убеждаемся, что повторов не было.
		time.Sleep(10 * time.Minute)
		synctest.Wait()
		if n := handled.Load(); n != 1 {
			t.Fatalf("обработчик звался %d раз, want 1", n)
		}
		msgs, _ := b.Core().Store().ListMessages(ctx, a.PeerID(), 0)
		if len(msgs) != 1 {
			t.Fatalf("в истории %d сообщений, want 1", len(msgs))
		}

		st := a.Core().Outbox().Stats()
		if st.SendFailures == 0 {
			t.Fatal("попытки при мёртвом получателе обязаны гореть в счётчике")
		}
		if st.Delivered != 1 {
			t.Fatalf("Delivered = %d, want 1", st.Delivered)
		}
	})
}

// Поддельный ack от третьего узла не гасит чужую очередь: расчёт строго
// по паре (отправитель ack'а, msg_id).
func TestForgedAckIgnored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 3)
		a, b, evil := c.Node(0), c.Node(1), c.Node(2)
		ctx := context.Background()

		c.Kill(1)
		mid := SendChat(t, a, b.PeerID(), "только для B")
		time.Sleep(20 * time.Second)
		synctest.Wait()

		// Узел-злодей подделывает ack на чужой msg_id.
		forged, err := envelope.Encode(envelope.Envelope{
			MsgID: envelope.MsgID{0xEE, 1}, Type: envelope.TypeAck, Payload: mid[:],
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := evil.Core().Node().SendDirect(ctx, a.ID(), forged); err != nil {
			t.Fatalf("отправка подделки: %v", err)
		}
		time.Sleep(30 * time.Second)
		synctest.Wait()

		m, ok, _ := a.Core().Store().GetMessage(ctx, b.PeerID(), mid, true)
		if !ok || m.Status != store.StatusQueued {
			t.Fatalf("подделка сработала: status=%v ok=%v", m.Status, ok)
		}
		if n, _ := a.Core().Store().OutboxPending(ctx, b.PeerID()); n != 1 {
			t.Fatalf("очередь к B: %d, want 1", n)
		}
		st := a.Core().Outbox().Stats()
		if st.Delivered != 0 {
			t.Fatalf("Delivered = %d, want 0", st.Delivered)
		}
		if st.AcksUnknown+st.GateDropped == 0 {
			t.Fatal("подделка обязана быть видна в счётчиках")
		}
	})
}

// Рестарт отправителя не теряет очередь: сообщение, отправленное до его
// смерти, доезжает после возвращения обоих.
func TestSenderRestartKeepsQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 2, WithSetup(func(i int, core *app.Core) {
			if i == 1 {
				RecordChat(core, nil)
			}
		}))
		a, b := c.Node(0), c.Node(1)

		c.Kill(1)
		mid := SendChat(t, a, b.PeerID(), "переживу рестарт")
		time.Sleep(10 * time.Second) // пара сгоревших попыток
		c.Kill(0)

		c.Restart(0)
		c.Restart(1)

		WaitMessageStatus(t, a, b.PeerID(), mid, store.StatusDelivered, 15*time.Minute)
		got := WaitInboundMessage(t, b, a.PeerID(), mid, time.Minute)
		if !bytes.Equal(got.Body, []byte("переживу рестарт")) {
			t.Fatalf("тело: %q", got.Body)
		}
	})
}

// Локально удалённое сообщение не воскресает пере-доставкой того же
// конверта: дедуп-окно живёт отдельно от истории.
func TestDeletedNotResurrected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var handled atomic.Int64
		c := NewCluster(t, 2, WithSetup(func(i int, core *app.Core) {
			if i == 1 {
				RecordChat(core, func() error { handled.Add(1); return nil })
			}
		}))
		a, b := c.Node(0), c.Node(1)
		ctx := context.Background()

		mid := SendChat(t, a, b.PeerID(), "сотри меня")
		WaitMessageStatus(t, a, b.PeerID(), mid, store.StatusDelivered, 5*time.Minute)
		WaitInboundMessage(t, b, a.PeerID(), mid, time.Minute)

		// B стирает контент локально.
		err := b.Core().Store().Tx(ctx, func(tx *store.Tx) error {
			return tx.DeleteMessageBody(a.PeerID(), mid)
		})
		if err != nil {
			t.Fatal(err)
		}

		// A переигрывает тот же конверт руками (злая пере-доставка).
		seqEnv := envelope.Envelope{
			MsgID: mid, Type: envelope.TypeChat, FromSeq: 1, LamportTS: 1,
			Payload: []byte("сотри меня"),
		}
		frame, err := envelope.Encode(seqEnv)
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Core().Node().SendDirect(ctx, b.ID(), frame); err != nil {
			t.Fatalf("пере-доставка: %v", err)
		}
		time.Sleep(30 * time.Second)
		synctest.Wait()

		m, ok, err := b.Core().Store().GetMessage(ctx, a.PeerID(), mid, false)
		if err != nil || !ok {
			t.Fatalf("сообщение пропало целиком: %v %v", ok, err)
		}
		if !m.Deleted || m.Body != nil {
			t.Fatalf("удалённое воскресло: %+v", m)
		}
		if n := handled.Load(); n != 1 {
			t.Fatalf("обработчик звался %d раз, want 1 (дедуп обязан гасить)", n)
		}
		if got := b.Core().Outbox().Stats().DedupHits; got < 1 {
			t.Fatalf("DedupHits = %d, want >= 1", got)
		}
	})
}

// Ошибка обработчика откатывает транзакцию целиком (включая дедуп) — ретрай
// отправителя переигрывает доставку без потерь и без дублей.
func TestHandlerErrorRollsBackAndRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		boom := errors.New("имитация падения посреди обработки")
		c := NewCluster(t, 2, WithSetup(func(i int, core *app.Core) {
			if i == 1 {
				RecordChat(core, func() error {
					if calls.Add(1) == 1 {
						return boom // первая доставка «падает»
					}
					return nil
				})
			}
		}))
		a, b := c.Node(0), c.Node(1)
		ctx := context.Background()

		mid := SendChat(t, a, b.PeerID(), "со второй попытки")
		WaitMessageStatus(t, a, b.PeerID(), mid, store.StatusDelivered, 15*time.Minute)

		got := WaitInboundMessage(t, b, a.PeerID(), mid, time.Minute)
		if !bytes.Equal(got.Body, []byte("со второй попытки")) {
			t.Fatalf("тело: %q", got.Body)
		}
		msgs, _ := b.Core().Store().ListMessages(ctx, a.PeerID(), 0)
		if len(msgs) != 1 {
			t.Fatalf("в истории %d сообщений, want 1", len(msgs))
		}
		if n := calls.Load(); n != 2 {
			t.Fatalf("обработчик звался %d раз, want 2 (падение + успех)", n)
		}
		if got := b.Core().Outbox().Stats().HandlerErrors; got != 1 {
			t.Fatalf("HandlerErrors = %d, want 1", got)
		}
	})
}
