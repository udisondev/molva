package media

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/udisondev/nodenet/kad"
	"github.com/udisondev/nodenet/transport"
	"github.com/udisondev/nodenet/transport/mem"
)

// pair — две медиасессии поверх mem-хаба, без узлов (чистый транспорт).
func pair(t testing.TB) (out, in transport.MediaSession) {
	t.Helper()
	hub := mem.NewHub()
	idA, idB := kad.ID{0xA}, kad.ID{0xB}
	addrA := transport.Addr{Net: "mem", Endpoint: "a"}
	addrB := transport.Addr{Net: "mem", Endpoint: "b"}
	trA, err := hub.New(idA, addrA)
	if err != nil {
		t.Fatal(err)
	}
	trB, err := hub.New(idB, addrB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = trA.Close(); _ = trB.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sess, err := trA.(transport.Media).OpenMedia(ctx, idB, addrB)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case in = <-trB.(transport.Media).InboundMedia():
	case <-ctx.Done():
		t.Fatal("входящая сессия не пришла")
	}
	return sess, in
}

func TestBridgeRoundTrip(t *testing.T) {
	out, in := pair(t)

	var rx atomic.Int64
	recv := NewBridge(func(ch uint8, _ time.Time, payload []byte) {
		if ch == 16 && bytes.Equal(payload, []byte("кадр")) {
			rx.Add(1)
		}
	}, nil)
	recv.Attach(in)

	send := NewBridge(nil, nil)
	send.Attach(out)
	for range 10 {
		if err := send.Send(16, []byte("кадр")); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for rx.Load() < 10 {
		if time.Now().After(deadline) {
			t.Fatalf("дошло %d из 10", rx.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Detach: смерть пути видна второй стороне.
	closed := make(chan struct{})
	recv2 := NewBridge(nil, func() { close(closed) })
	_ = recv2 // onClosed вешается на отправную сторону ниже
	sendClosed := make(chan struct{})
	send.onClosed = func() { close(sendClosed) }
	recv.Detach()
	select {
	case <-sendClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("смерть пути не доехала до отправителя")
	}
	if send.Active() {
		t.Fatal("мост обязан отцепиться")
	}
}

// Бюджет медиапути: кадр из UI до датаграммы транспорта — без аллокаций.
func BenchmarkBridgeSend(b *testing.B) {
	out, in := pair(b)
	go func() {
		for dg := range in.Datagrams() {
			dg.Pkt.Release()
		}
	}()
	br := NewBridge(nil, nil)
	br.Attach(out)
	frame := bytes.Repeat([]byte{0xAB}, 320) // типичный opus-кадр

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for {
			err := br.Send(16, frame)
			if err == nil {
				break
			}
			if errors.Is(err, transport.ErrMediaBackpressure) {
				// Штатный сигнал перегруза: бенч молотит быстрее 50 кадров/с.
				runtime.Gosched()
				continue
			}
			b.Fatal(err)
		}
	}
}
