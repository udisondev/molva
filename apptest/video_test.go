package apptest

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"testing/synctest"
	"time"

	"github.com/udisondev/molva/callsig"
	"github.com/udisondev/molva/media"
)

// Видеокадр крупнее кадра nodenet дробится, едет надёжными сообщениями и
// собирается целиком; аудио продолжает ходить датаграммами рядом.
func TestVideoFrameOverCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 2)
		a, b := c.Node(0), c.Node(1)
		ctx := context.Background()
		makeMesh(t, c, 0, 1)

		callID, err := a.Core().Calls().Start(ctx, b.PeerID(), []string{"opus", "vp8"})
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Minute)
		for {
			synctest.Wait()
			if cur, ok := b.Core().Calls().Current(); ok && cur.State == callsig.StateRingingIn {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("входящий не дошёл")
			}
			time.Sleep(time.Second)
		}
		if err := b.Core().Calls().Accept(ctx, callID); err != nil {
			t.Fatal(err)
		}
		waitMediaActive(t, a, true)
		waitMediaActive(t, b, true)

		// «Ключевой кадр» 200 КиБ — 4 сегмента.
		frame := make([]byte, 200_000)
		if _, err := rand.Read(frame); err != nil {
			t.Fatal(err)
		}
		if err := a.Core().Media().Send(media.ChVideo, frame); err != nil {
			t.Fatalf("Send video: %v", err)
		}

		deadline = time.Now().Add(2 * time.Minute)
		for {
			synctest.Wait()
			select {
			case f := <-b.MediaIn():
				if f.Ch != media.ChVideo {
					continue
				}
				if !bytes.Equal(f.Payload, frame) {
					t.Fatalf("кадр разошёлся: %d байт", len(f.Payload))
				}
				return
			default:
			}
			if time.Now().After(deadline) {
				t.Fatalf("видеокадр не собрался: %+v", b.Core().Media().Stats())
			}
			time.Sleep(200 * time.Millisecond)
		}
	})
}
