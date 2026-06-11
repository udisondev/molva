package apptest

import (
	"bytes"
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/udisondev/nodenet/transport/mem"
)

// Звонок под потерями и джиттером: ретрансмиссий нет by design, но
// существенная доля кадров доходит, путь живёт, ничего не виснет.
func TestCallUnderLossAndJitter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 2)
		a, b := c.Node(0), c.Node(1)
		ctx := context.Background()
		makeMesh(t, c, 0, 1)

		callID, err := a.Core().Calls().Start(ctx, b.PeerID(), []string{"opus"})
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Minute)
		for {
			synctest.Wait()
			if cur, ok := b.Core().Calls().Current(); ok && cur.ID == callID {
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

		// Грязный линк в сторону B: 30% потерь, джиттер до 25 мс.
		c.SetLinkProfile(0, 1, mem.LinkProfile{Seed: 7, Loss: 0.3, Jitter: 25 * time.Millisecond})

		frame := bytes.Repeat([]byte{0x5A}, 320)
		const total = 200
		for range total {
			_ = a.Core().Media().Send(16, frame) // backpressure не страшен: PLC
			time.Sleep(20 * time.Millisecond)    // темп opus-кадров
		}

		// Дать джиттеру дозвучать.
		time.Sleep(2 * time.Second)
		synctest.Wait()
		got := 0
		for {
			select {
			case <-b.MediaIn():
				got++
				continue
			default:
			}
			break
		}
		// При 30% потерь ожидаем порядка 140; допуск широкий, но потери
		// обязаны быть видны (не 100%) и связь жива (не 0%).
		if got < total/2 || got >= total {
			t.Fatalf("дошло %d из %d", got, total)
		}
		if !a.Core().Media().Active() || !b.Core().Media().Active() {
			t.Fatal("путь не должен умирать от потерь")
		}
	})
}
