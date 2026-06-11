package apptest

import (
	"bytes"
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/store"
)

// sendText шлёт через штатный chat-движок и падает тестом при ошибке.
func sendText(t *testing.T, n *Node, to peer.ID, text string) envelope.MsgID {
	t.Helper()
	mid, err := n.Core().Chats().SendText(context.Background(), to, text)
	if err != nil {
		t.Fatalf("SendText: %v", err)
	}
	return mid
}

// Полный путь мессенджера: знакомство по инвайту, интерактивная сессия,
// шифрованная переписка в обе стороны; plaintext не появляется на проводе.
func TestMessengerEndToEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 2)
		a, b := c.Node(0), c.Node(1)
		MakeContacts(t, c, 0, 1)

		const secretB = "тайна от B к A: пароль-9000"
		const secretA = "ответная тайна от A к B"

		// B пишет первым: рукопожатие инициируется им автоматически.
		mid1 := sendText(t, b, a.PeerID(), secretB)
		WaitMessageStatus(t, b, a.PeerID(), mid1, store.StatusDelivered, 5*time.Minute)
		got := WaitInboundMessage(t, a, b.PeerID(), mid1, time.Minute)
		if !bytes.Equal(got.Body, []byte(secretB)) {
			t.Fatalf("тело у A: %q", got.Body)
		}

		// A отвечает по той же сессии.
		mid2 := sendText(t, a, b.PeerID(), secretA)
		WaitMessageStatus(t, a, b.PeerID(), mid2, store.StatusDelivered, 5*time.Minute)
		got = WaitInboundMessage(t, b, a.PeerID(), mid2, time.Minute)
		if !bytes.Equal(got.Body, []byte(secretA)) {
			t.Fatalf("тело у B: %q", got.Body)
		}

		// Перехват уровня nodenet не видит открытого текста.
		for _, n := range []*Node{a, b} {
			for {
				select {
				case d := <-n.Inbox():
					if bytes.Contains(d.Payload, []byte(secretB)) || bytes.Contains(d.Payload, []byte(secretA)) {
						t.Fatal("открытый текст на проводе")
					}
					continue
				default:
				}
				break
			}
		}
	})
}

// Заблокированный дропается без ack и видит вечный offline; история
// блокирующего не растёт.
func TestBlockedSilence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 2)
		a, b := c.Node(0), c.Node(1)
		ctx := context.Background()
		MakeContacts(t, c, 0, 1)

		// Разогрев: сессия установлена, обмен случился.
		mid := sendText(t, b, a.PeerID(), "до блокировки")
		WaitInboundMessage(t, a, b.PeerID(), mid, 5*time.Minute)

		if err := a.Core().Contacts().Block(ctx, b.PeerID()); err != nil {
			t.Fatalf("Block: %v", err)
		}
		baseGate := a.Core().Outbox().Stats().GateDropped
		histBefore, _ := a.Core().Store().ListMessages(ctx, b.PeerID(), 0)

		// B пишет в пустоту: ack не приходит, статус навсегда queued/sent.
		mid2 := sendText(t, b, a.PeerID(), "в пустоту")
		time.Sleep(10 * time.Minute)
		synctest.Wait()

		m, ok, _ := b.Core().Store().GetMessage(ctx, a.PeerID(), mid2, true)
		if !ok || m.Status == store.StatusDelivered {
			t.Fatalf("заблокированный получил delivered: %+v ok=%v", m, ok)
		}
		histAfter, _ := a.Core().Store().ListMessages(ctx, b.PeerID(), 0)
		if len(histAfter) != len(histBefore) {
			t.Fatalf("история блокирующего выросла: %d -> %d", len(histBefore), len(histAfter))
		}
		if a.Core().Outbox().Stats().GateDropped <= baseGate {
			t.Fatal("дропы блокировки обязаны быть видны в счётчике")
		}
		// Вечный offline с точки зрения заблокированного.
		if b.Core().Contacts().Online(a.PeerID()) {
			t.Fatal("заблокированный видит онлайн")
		}
	})
}

// Pong не отдаётся незнакомцу: presence не утекает за пределы взаимных
// контактов.
func TestPongNotForStrangers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 3)
		a, stranger := c.Node(0), c.Node(2)
		MakeContacts(t, c, 0, 1)
		ctx := context.Background()

		// Незнакомец зондирует A напрямую.
		mid, _ := envelope.NewMsgID(bytes.NewReader(bytes.Repeat([]byte{7}, 16)))
		probe, err := envelope.Encode(envelope.Envelope{MsgID: mid, Type: envelope.TypeProbe})
		if err != nil {
			t.Fatal(err)
		}
		if err := stranger.Core().Node().SendDirect(ctx, a.ID(), probe); err != nil {
			t.Fatalf("зонд: %v", err)
		}
		time.Sleep(30 * time.Second)
		synctest.Wait()

		// Ответа нет: в перехвате незнакомца не должно быть PONG от A.
		for {
			select {
			case d := <-stranger.Inbox():
				if d.From == a.ID() {
					if env, err := envelope.Decode(d.Payload); err == nil && env.Type == envelope.TypePong {
						t.Fatal("pong утёк незнакомцу")
					}
				}
				continue
			default:
			}
			break
		}
	})
}

// Presence: контакты видят онлайн друг друга, смерть узла переводит его в
// офлайн по TTL, возвращение — обратно в онлайн.
func TestPresenceOnlineOffline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 2)
		a, b := c.Node(0), c.Node(1)
		MakeContacts(t, c, 0, 1)

		waitOnline := func(on *Node, who *Node, want bool) {
			t.Helper()
			deadline := time.Now().Add(10 * time.Minute)
			for {
				synctest.Wait()
				if on.Core().Contacts().Online(who.PeerID()) == want {
					return
				}
				if time.Now().After(deadline) {
					t.Fatalf("node-%d не увидел node-%d online=%v", on.index, who.index, want)
				}
				time.Sleep(5 * time.Second)
			}
		}

		waitOnline(a, b, true)
		waitOnline(b, a, true)

		c.Kill(1)
		waitOnline(a, b, false)

		c.Restart(1)
		waitOnline(a, b, true)
	})
}

// Одновременные рукопожатия: обе стороны пишут первыми без сессии;
// коллизию решает меньший NodeID, оба сообщения доставляются ровно один
// раз, дальнейшая переписка живёт в одной сессии.
func TestSimultaneousInitCollision(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 2)
		a, b := c.Node(0), c.Node(1)
		ctx := context.Background()
		MakeContacts(t, c, 0, 1)

		midA := sendText(t, a, b.PeerID(), "от A одновременно")
		midB := sendText(t, b, a.PeerID(), "от B одновременно")

		WaitMessageStatus(t, a, b.PeerID(), midA, store.StatusDelivered, 15*time.Minute)
		WaitMessageStatus(t, b, a.PeerID(), midB, store.StatusDelivered, 15*time.Minute)
		WaitInboundMessage(t, b, a.PeerID(), midA, time.Minute)
		WaitInboundMessage(t, a, b.PeerID(), midB, time.Minute)

		// Ровно по одному в каждой истории.
		if msgs, _ := a.Core().Store().ListMessages(ctx, b.PeerID(), 0); len(msgs) != 2 {
			t.Fatalf("история A: %d записей, want 2", len(msgs))
		}
		if msgs, _ := b.Core().Store().ListMessages(ctx, a.PeerID(), 0); len(msgs) != 2 {
			t.Fatalf("история B: %d записей, want 2", len(msgs))
		}

		// Переписка продолжается.
		mid3 := sendText(t, a, b.PeerID(), "после коллизии")
		WaitInboundMessage(t, b, a.PeerID(), mid3, 5*time.Minute)
	})
}

// Потеря состояния сессии у получателя лечится протокольным re-handshake;
// следующие сообщения доставляются.
func TestSessionLossRehandshake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 2)
		a, b := c.Node(0), c.Node(1)
		ctx := context.Background()
		MakeContacts(t, c, 0, 1)

		mid1 := sendText(t, a, b.PeerID(), "первое")
		WaitInboundMessage(t, b, a.PeerID(), mid1, 5*time.Minute)

		// B теряет состояние сессии (имитация порчи/отката диска).
		err := b.Core().Store().Tx(ctx, func(tx *store.Tx) error {
			return tx.SessionDelete(a.PeerID())
		})
		if err != nil {
			t.Fatal(err)
		}

		// Это сообщение упадёт в рассинхрон у B и честно потеряется,
		// запустив лечение.
		sendText(t, a, b.PeerID(), "потеряется")
		time.Sleep(2 * time.Minute)
		synctest.Wait()

		st := b.Core().Chats().Stats()
		if st.NoSession+st.DecryptFailures == 0 {
			t.Fatal("рассинхрон обязан быть виден в счётчиках")
		}

		// После лечения переписка работает в обе стороны.
		mid3 := sendText(t, a, b.PeerID(), "после лечения")
		WaitInboundMessage(t, b, a.PeerID(), mid3, 15*time.Minute)
		mid4 := sendText(t, b, a.PeerID(), "и обратно")
		WaitInboundMessage(t, a, b.PeerID(), mid4, 15*time.Minute)
	})
}
