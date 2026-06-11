package apptest

import (
	"bytes"
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/udisondev/molva/callsig"
)

// waitCallState ждёт состояние want у текущего звонка узла.
func waitCallState(t *testing.T, n *Node, want callsig.State) callsig.Call {
	t.Helper()
	deadline := time.Now().Add(5 * time.Minute)
	for {
		synctest.Wait()
		if c, ok := n.Core().Calls().Current(); ok && c.State == want {
			return c
		}
		if time.Now().After(deadline) {
			c, ok := n.Core().Calls().Current()
			t.Fatalf("node-%d: состояние %d не достигнуто (current=%+v ok=%v)", n.index, want, c, ok)
		}
		time.Sleep(time.Second)
	}
}

// waitMediaActive ждёт живой медиамост.
func waitMediaActive(t *testing.T, n *Node, want bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Minute)
	for {
		synctest.Wait()
		if n.Core().Media().Active() == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node-%d: мост active != %v", n.index, want)
		}
		time.Sleep(time.Second)
	}
}

// Полный звонок: сигналинг, consent, медиапуть, кадры в обе стороны с
// RxTime, hangup гасит мост у обоих.
func TestCallEndToEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 2)
		a, b := c.Node(0), c.Node(1)
		ctx := context.Background()
		makeMesh(t, c, 0, 1)

		callID, err := a.Core().Calls().Start(ctx, b.PeerID(), []string{"opus"})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}

		// B видит входящий и принимает.
		var incoming callsig.Call
		deadline := time.Now().Add(5 * time.Minute)
		for {
			synctest.Wait()
			select {
			case incoming = <-b.CallsIn():
			default:
			}
			if incoming.ID == callID || time.Now().After(deadline) {
				break
			}
			time.Sleep(time.Second)
		}
		if incoming.ID != callID {
			t.Fatal("входящий звонок не дошёл")
		}
		if err := b.Core().Calls().Accept(ctx, callID); err != nil {
			t.Fatalf("Accept: %v", err)
		}

		waitCallState(t, a, callsig.StateActive)
		waitMediaActive(t, a, true)
		waitMediaActive(t, b, true)

		// Кадры в обе стороны.
		frame := bytes.Repeat([]byte{0xAB}, 320) // «opus-кадр»
		for range 5 {
			if err := a.Core().Media().Send(16, frame); err != nil {
				t.Fatalf("Send A: %v", err)
			}
		}
		got := 0
		deadline = time.Now().Add(2 * time.Minute)
		for got < 5 {
			synctest.Wait()
			select {
			case f := <-b.MediaIn():
				if f.Ch != 16 || !bytes.Equal(f.Payload, frame) || f.Rx.IsZero() {
					t.Fatalf("кадр у B: ch=%d len=%d rx=%v", f.Ch, len(f.Payload), f.Rx)
				}
				got++
				continue
			default:
			}
			if time.Now().After(deadline) {
				t.Fatalf("дошло %d кадров из 5; stats=%+v", got, b.Core().Media().Stats())
			}
			time.Sleep(200 * time.Millisecond)
		}

		if err := b.Core().Media().Send(17, frame); err != nil {
			t.Fatalf("Send B: %v", err)
		}
		deadline = time.Now().Add(2 * time.Minute)
		for {
			synctest.Wait()
			select {
			case f := <-a.MediaIn():
				if f.Ch != 17 {
					t.Fatalf("канал: %d", f.Ch)
				}
				goto hangup
			default:
			}
			if time.Now().After(deadline) {
				t.Fatal("обратный кадр не дошёл")
			}
			time.Sleep(200 * time.Millisecond)
		}

	hangup:
		if err := a.Core().Calls().Hangup(ctx, callID); err != nil {
			t.Fatalf("Hangup: %v", err)
		}
		waitMediaActive(t, a, false)
		// B узнаёт о hangup сигналингом.
		deadline = time.Now().Add(5 * time.Minute)
		for {
			synctest.Wait()
			if _, ok := b.Core().Calls().Current(); !ok {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("B не увидел hangup")
			}
			time.Sleep(time.Second)
		}
		waitMediaActive(t, b, false)
	})
}

// Отказ: медиапуть не открывается, звонящий видит ended.
func TestCallRejected(t *testing.T) {
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
			if cur, ok := b.Core().Calls().Current(); ok && cur.State == callsig.StateRingingIn {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("входящий не дошёл")
			}
			time.Sleep(time.Second)
		}
		if err := b.Core().Calls().Reject(ctx, callID); err != nil {
			t.Fatal(err)
		}

		deadline = time.Now().Add(5 * time.Minute)
		for {
			synctest.Wait()
			if _, ok := a.Core().Calls().Current(); !ok {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("A не увидел отказ")
			}
			time.Sleep(time.Second)
		}
		if a.Core().Media().Active() || b.Core().Media().Active() {
			t.Fatal("медиапуть не должен был открываться")
		}
	})
}

// Смерть медиапути при живом звонке: звонивший немедленно переоткрывает
// (make-before-break), кадры снова ходят.
func TestCallPathReopen(t *testing.T) {
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

		// Путь умирает на стороне ответившего (закрытие сессии).
		b.Core().Media().Detach()

		// Звонивший переоткрывает; новые кадры доходят.
		waitMediaActive(t, b, true)
		waitMediaActive(t, a, true)
		frame := bytes.Repeat([]byte{0xCD}, 200)
		deadline = time.Now().Add(2 * time.Minute)
		for {
			synctest.Wait()
			_ = a.Core().Media().Send(16, frame)
			select {
			case f := <-b.MediaIn():
				if bytes.Equal(f.Payload, frame) {
					return
				}
			default:
			}
			if time.Now().After(deadline) {
				t.Fatalf("кадры после переоткрытия не дошли: %+v", a.Core().Media().Stats())
			}
			time.Sleep(time.Second)
		}
	})
}

// Звонок сразу после добавления инвайта: DR-сессии ещё нет, offer
// досылается сам после рукопожатия — у B звонит без каких-либо действий
// с его стороны.
func TestCallRightAfterInvite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 2)
		a, b := c.Node(0), c.Node(1)
		ctx := context.Background()

		invite := b.Core().Contacts().MyInvite("")
		if _, err := a.Core().Contacts().AddByInvite(ctx, invite); err != nil {
			t.Fatalf("AddByInvite: %v", err)
		}
		callID, err := a.Core().Calls().Start(ctx, b.PeerID(), []string{"opus"})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}

		var incoming callsig.Call
		deadline := time.Now().Add(2 * time.Minute)
		for {
			synctest.Wait()
			select {
			case incoming = <-b.CallsIn():
			default:
			}
			if incoming.ID == callID {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("входящий звонок не дошёл")
			}
			time.Sleep(time.Second)
		}
		if err := b.Core().Calls().Accept(ctx, callID); err != nil {
			t.Fatalf("Accept: %v", err)
		}
		waitCallState(t, a, callsig.StateActive)
	})
}
