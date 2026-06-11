package apptest

import (
	"context"
	"crypto/rand"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/ratchet"
	"github.com/udisondev/molva/store"
)

// Транзакционная связка ratchet+store: crash посреди обработки входящего
// (откат транзакции) не рассинхронизирует сессию — повторная доставка того
// же сообщения расшифровывается, ничего не теряется и не дублируется.
func TestRatchetStoreCrashConsistency(t *testing.T) {
	ctx := context.Background()
	idA := peer.ID{0xA1}
	idB := peer.ID{0xB2}

	dbB, err := store.Open(filepath.Join(t.TempDir(), "b.db"), store.KeyFromSeed([32]byte{0xB}))
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()

	// Интерактивная инициализация: A — инициатор.
	h, err := ratchet.NewHandshake(rand.Reader, ratchet.IdentityFromSeed([32]byte{1}))
	if err != nil {
		t.Fatal(err)
	}
	stB, ack, err := ratchet.AcceptHandshake(rand.Reader, ratchet.IdentityFromSeed([32]byte{2}), h.Init(), idA, idB)
	if err != nil {
		t.Fatal(err)
	}
	stA, err := h.Finish(rand.Reader, ack, idA, idB)
	if err != nil {
		t.Fatal(err)
	}
	// B сохраняет своё состояние, как сделал бы обработчик SESSION_INIT.
	blob, err := stB.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	err = dbB.Tx(ctx, func(tx *store.Tx) error { return tx.SessionPut(idA, blob, 1) })
	if err != nil {
		t.Fatal(err)
	}

	// receive прогоняет полную транзакцию обработки входящего CHAT у B:
	// {дедуп, загрузка сессии, расшифровка, сохранение сессии, история};
	// fail имитирует crash до коммита.
	receive := func(m ratchet.Message, mid envelope.MsgID, fail bool) (string, error) {
		var text string
		err := dbB.Tx(ctx, func(tx *store.Tx) error {
			fresh, err := tx.DedupInsert(idA, mid, time.Now().UnixMilli())
			if err != nil {
				return err
			}
			if !fresh {
				return nil
			}
			raw, ok, err := tx.SessionGet(idA)
			if err != nil || !ok {
				t.Fatalf("сессия пропала: %v %v", ok, err)
			}
			st, err := ratchet.Unmarshal(raw)
			if err != nil {
				return err
			}
			plain, err := st.Decrypt(rand.Reader, m)
			if err != nil {
				return err
			}
			next, err := st.Marshal()
			if err != nil {
				return err
			}
			if err := tx.SessionPut(idA, next, time.Now().UnixMilli()); err != nil {
				return err
			}
			if _, err := tx.InsertMessage(&store.Message{
				Peer: idA, MsgID: mid, Outgoing: false, SentAt: time.Now().UnixMilli(),
				Status: store.StatusDelivered, Body: plain,
			}); err != nil {
				return err
			}
			text = string(plain)
			if fail {
				return errors.New("crash до коммита")
			}
			return nil
		})
		return text, err
	}

	mid1 := envelope.MsgID{1}
	m1, err := stA.Encrypt([]byte("первое"))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := receive(m1, mid1, false); err != nil || got != "первое" {
		t.Fatalf("m1: %q %v", got, err)
	}

	// Второе сообщение: первая обработка «падает» перед коммитом.
	mid2 := envelope.MsgID{2}
	m2, err := stA.Encrypt([]byte("второе"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := receive(m2, mid2, true); err == nil {
		t.Fatal("имитация crash обязана вернуть ошибку")
	}
	// Повторная доставка того же конверта: дедуп откатился, сессия не
	// продвинулась — расшифровка работает.
	if got, err := receive(m2, mid2, false); err != nil || got != "второе" {
		t.Fatalf("повтор m2: %q %v", got, err)
	}

	// Ровно одна запись на сообщение, обе расшифрованы.
	msgs, err := dbB.ListMessages(ctx, idA, 0)
	if err != nil || len(msgs) != 2 {
		t.Fatalf("история: %d записей, %v", len(msgs), err)
	}

	// Сессия жива и шагает дальше: B отвечает, A читает.
	var reply ratchet.Message
	err = dbB.Tx(ctx, func(tx *store.Tx) error {
		raw, ok, err := tx.SessionGet(idA)
		if err != nil || !ok {
			return errors.New("сессии нет")
		}
		st, err := ratchet.Unmarshal(raw)
		if err != nil {
			return err
		}
		reply, err = st.Encrypt([]byte("ответ B"))
		if err != nil {
			return err
		}
		next, err := st.Marshal()
		if err != nil {
			return err
		}
		return tx.SessionPut(idA, next, time.Now().UnixMilli())
	})
	if err != nil {
		t.Fatal(err)
	}
	plain, err := stA.Decrypt(rand.Reader, reply)
	if err != nil || string(plain) != "ответ B" {
		t.Fatalf("ответ: %q %v", plain, err)
	}
}
