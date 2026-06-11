package store

import (
	"context"
	"crypto/rand"
	"path/filepath"
	"testing"

	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
)

// Бюджет транзакции входящего сообщения: дедуп + продвижение
// крипто-состояния + запись истории одной транзакцией.
func BenchmarkInboundMessageTx(b *testing.B) {
	d, err := Open(filepath.Join(b.TempDir(), "bench.db"), KeyFromSeed([32]byte{1}))
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	ctx := context.Background()
	from := peer.ID{0xBE}
	state := make([]byte, 512) // типичный размер ratchet-состояния
	if _, err := rand.Read(state); err != nil {
		b.Fatal(err)
	}
	body := []byte("типичное сообщение длиной в одну строку — без вложений")

	b.ReportAllocs()
	b.ResetTimer()
	var i int64
	for b.Loop() {
		i++
		var mid envelope.MsgID
		for j := range 8 {
			mid[j] = byte(i >> (8 * j))
		}
		err := d.Tx(ctx, func(tx *Tx) error {
			if _, err := tx.DedupInsert(from, mid, i); err != nil {
				return err
			}
			if err := tx.SessionPut(from, state, i); err != nil {
				return err
			}
			if err := tx.LamportObserve(uint64(i)); err != nil {
				return err
			}
			_, err := tx.InsertMessage(&Message{
				Peer: from, MsgID: mid, Outgoing: false, FromSeq: uint64(i),
				Lamport: uint64(i), SentAt: i, Status: StatusDelivered, Body: body,
			})
			return err
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
