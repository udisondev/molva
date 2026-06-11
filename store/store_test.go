package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
)

var (
	peerA = peer.ID{0xAA, 1, 2, 3}
	midX  = envelope.MsgID{0x11, 1}
	midY  = envelope.MsgID{0x22, 2}
)

func openTemp(t *testing.T, key [32]byte) (*DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "molva.db")
	d, err := Open(path, key)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d, path
}

func TestOpenWrongKey(t *testing.T) {
	key1 := KeyFromSeed([32]byte{1})
	key2 := KeyFromSeed([32]byte{2})
	d, path := openTemp(t, key1)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, key2); !errors.Is(err, ErrWrongKey) {
		t.Fatalf("чужой ключ: err = %v, want ErrWrongKey", err)
	}
	// Правильный ключ продолжает работать.
	d2, err := Open(path, key1)
	if err != nil {
		t.Fatalf("повторное открытие: %v", err)
	}
	_ = d2.Close()
}

func TestMessageRoundTripAndStatus(t *testing.T) {
	d, _ := openTemp(t, KeyFromSeed([32]byte{3}))
	ctx := context.Background()

	err := d.Tx(ctx, func(tx *Tx) error {
		ins, err := tx.InsertMessage(&Message{
			Peer: peerA, MsgID: midX, Outgoing: true, FromSeq: 1, Lamport: 5,
			SentAt: 1000, Status: StatusQueued, Body: []byte("привет"),
		})
		if err != nil {
			return err
		}
		if !ins {
			t.Fatal("первая вставка обязана пройти")
		}
		// Повторная вставка — не ошибка, а дубль (идемпотентность).
		ins, err = tx.InsertMessage(&Message{
			Peer: peerA, MsgID: midX, Outgoing: true, SentAt: 1, Status: StatusQueued, Body: []byte("дубль"),
		})
		if err != nil {
			return err
		}
		if ins {
			t.Fatal("повторная вставка обязана быть дублем")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	got, ok, err := d.GetMessage(ctx, peerA, midX, true)
	if err != nil || !ok {
		t.Fatalf("GetMessage: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got.Body, []byte("привет")) || got.Status != StatusQueued || got.FromSeq != 1 {
		t.Fatalf("не то прочлось: %+v", got)
	}

	// Статусы монотонны: понижение не проходит.
	if err := d.Tx(ctx, func(tx *Tx) error { return tx.MessageStatusUp(peerA, midX, StatusDelivered) }); err != nil {
		t.Fatal(err)
	}
	if err := d.Tx(ctx, func(tx *Tx) error { return tx.MessageStatusUp(peerA, midX, StatusSent) }); err != nil {
		t.Fatal(err)
	}
	got, _, _ = d.GetMessage(ctx, peerA, midX, true)
	if got.Status != StatusDelivered {
		t.Fatalf("статус понизился: %v", got.Status)
	}
}

func TestDeleteMessageBody(t *testing.T) {
	d, _ := openTemp(t, KeyFromSeed([32]byte{4}))
	ctx := context.Background()

	err := d.Tx(ctx, func(tx *Tx) error {
		if _, err := tx.InsertMessage(&Message{
			Peer: peerA, MsgID: midX, Outgoing: false, SentAt: 1, Status: StatusDelivered,
			Body: []byte("секрет"),
		}); err != nil {
			return err
		}
		if _, err := tx.DedupInsert(peerA, midX, 1); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := d.Tx(ctx, func(tx *Tx) error { return tx.DeleteMessageBody(peerA, midX) }); err != nil {
		t.Fatal(err)
	}
	got, ok, _ := d.GetMessage(ctx, peerA, midX, false)
	if !ok || !got.Deleted || got.Body != nil {
		t.Fatalf("удаление не сработало: %+v", got)
	}
	// Дедуп-окно не чистится удалением — пере-доставка не воскресит.
	seen, err := d.DedupSeen(ctx, peerA, midX)
	if err != nil || !seen {
		t.Fatalf("дедуп-запись пропала: seen=%v err=%v", seen, err)
	}
}

func TestDedupWindow(t *testing.T) {
	d, _ := openTemp(t, KeyFromSeed([32]byte{5}))
	ctx := context.Background()

	err := d.Tx(ctx, func(tx *Tx) error {
		fresh, err := tx.DedupInsert(peerA, midX, 100)
		if err != nil || !fresh {
			t.Fatalf("первый раз должен быть свежим: %v %v", fresh, err)
		}
		fresh, err = tx.DedupInsert(peerA, midX, 200)
		if err != nil || fresh {
			t.Fatalf("второй раз должен быть дублем: %v %v", fresh, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Подрезка по возрасту и ёмкости.
	err = d.Tx(ctx, func(tx *Tx) error {
		if _, err := tx.DedupInsert(peerA, midY, 300); err != nil {
			return err
		}
		return tx.DedupPrune(peerA, 250, 100) // midX (seen 100) старее отсечки
	})
	if err != nil {
		t.Fatal(err)
	}
	seen, _ := d.DedupSeen(ctx, peerA, midX)
	if seen {
		t.Fatal("старая запись пережила подрезку по возрасту")
	}
	seen, _ = d.DedupSeen(ctx, peerA, midY)
	if !seen {
		t.Fatal("свежая запись не должна вылетать")
	}
}

func TestCounters(t *testing.T) {
	d, _ := openTemp(t, KeyFromSeed([32]byte{6}))
	ctx := context.Background()

	err := d.Tx(ctx, func(tx *Tx) error {
		for want := uint64(1); want <= 3; want++ {
			v, err := tx.NextSeq("seq:abc")
			if err != nil || v != want {
				t.Fatalf("NextSeq = %d, %v; want %d", v, err, want)
			}
		}
		// Лампорт: наблюдение большой чужой метки прыгает вперёд.
		if err := tx.LamportObserve(100); err != nil {
			return err
		}
		v, err := tx.LamportNext()
		if err != nil || v != 102 {
			t.Fatalf("lamport после observe(100) = %d, %v; want 102", v, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestOutboxLifecycle(t *testing.T) {
	d, _ := openTemp(t, KeyFromSeed([32]byte{7}))
	ctx := context.Background()
	frame := []byte("кадр-конверта")

	err := d.Tx(ctx, func(tx *Tx) error {
		return tx.OutboxEnqueue(peerA, midX, frame, 1000)
	})
	if err != nil {
		t.Fatal(err)
	}

	due, corrupt, err := d.OutboxDue(ctx, 1000, 10)
	if err != nil || len(due) != 1 || len(corrupt) != 0 {
		t.Fatalf("due: %v %v", due, err)
	}
	if !bytes.Equal(due[0].Frame, frame) || due[0].Peer != peerA || due[0].MsgID != midX {
		t.Fatalf("не тот элемент: %+v", due[0])
	}

	// Попытка → отъезд в будущее; kick возвращает в настоящее.
	if err := d.Tx(ctx, func(tx *Tx) error { return tx.OutboxAttempt(due[0].ID, 1, 6000) }); err != nil {
		t.Fatal(err)
	}
	if due, _, _ = d.OutboxDue(ctx, 1000, 10); len(due) != 0 {
		t.Fatal("после attempt элемент не должен быть due")
	}
	at, ok, _ := d.OutboxNearest(ctx)
	if !ok || at != 6000 {
		t.Fatalf("nearest = %d %v", at, ok)
	}
	if err := d.Tx(ctx, func(tx *Tx) error { return tx.OutboxKick(peerA, 2000) }); err != nil {
		t.Fatal(err)
	}
	if due, _, _ = d.OutboxDue(ctx, 2000, 10); len(due) != 1 || due[0].Attempts != 0 {
		t.Fatalf("kick не вернул в очередь: %+v", due)
	}

	// Ack снимает; чужой пир и повторный — нет.
	err = d.Tx(ctx, func(tx *Tx) error {
		if okk, err := tx.OutboxSettle(peer.ID{0xBB}, midX); err != nil || okk {
			t.Fatalf("settle чужим пиром обязан быть пустым: %v %v", okk, err)
		}
		okk, err := tx.OutboxSettle(peerA, midX)
		if err != nil || !okk {
			t.Fatalf("settle: %v %v", okk, err)
		}
		okk, err = tx.OutboxSettle(peerA, midX)
		if err != nil || okk {
			t.Fatalf("повторный settle должен быть пустым: %v %v", okk, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	n, _ := d.OutboxPending(ctx, peerA)
	if n != 0 {
		t.Fatalf("очередь не пуста: %d", n)
	}
}

func TestTxRollbackAtomicity(t *testing.T) {
	d, _ := openTemp(t, KeyFromSeed([32]byte{8}))
	ctx := context.Background()
	boom := errors.New("boom")

	err := d.Tx(ctx, func(tx *Tx) error {
		if _, err := tx.DedupInsert(peerA, midX, 1); err != nil {
			return err
		}
		if _, err := tx.InsertMessage(&Message{Peer: peerA, MsgID: midX, SentAt: 1, Status: StatusDelivered, Body: []byte("x")}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("ошибка fn должна вернуться: %v", err)
	}
	if seen, _ := d.DedupSeen(ctx, peerA, midX); seen {
		t.Fatal("откат не удалил дедуп-запись")
	}
	if _, ok, _ := d.GetMessage(ctx, peerA, midX, false); ok {
		t.Fatal("откат не удалил сообщение")
	}
}

// Контент-поля не лежат в файле открытым текстом.
func TestEncryptedAtRest(t *testing.T) {
	key := KeyFromSeed([32]byte{9})
	path := filepath.Join(t.TempDir(), "molva.db")
	d, err := Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	secret := []byte("СОВЕРШЕННО-СЕКРЕТНОЕ-ТЕЛО-0123456789")
	err = d.Tx(ctx, func(tx *Tx) error {
		if _, err := tx.InsertMessage(&Message{Peer: peerA, MsgID: midX, SentAt: 1, Status: StatusQueued, Body: secret}); err != nil {
			return err
		}
		return tx.OutboxEnqueue(peerA, midY, secret, 1)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{path, path + "-wal"} {
		raw, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		if bytes.Contains(raw, secret) {
			t.Fatalf("открытый текст найден в %s", f)
		}
	}
}
