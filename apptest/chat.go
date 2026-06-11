package apptest

import (
	"context"
	"crypto/rand"
	"testing"
	"testing/synctest"
	"time"

	"github.com/udisondev/molva/app"
	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/store"
)

// SendChat прототипирует путь отправки личного сообщения до появления
// пакета chat: запись истории и постановка в outbox одной транзакцией,
// затем пинок отправщику. Возвращает msg_id.
func SendChat(t *testing.T, n *Node, to peer.ID, text string) envelope.MsgID {
	t.Helper()
	core := n.Core()
	if core == nil {
		t.Fatalf("node-%d: мёртв", n.index)
	}
	mid, err := envelope.NewMsgID(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	err = core.Store().Tx(ctx, func(tx *store.Tx) error {
		seq, err := tx.NextSeq("seq:" + to.String())
		if err != nil {
			return err
		}
		lam, err := tx.LamportNext()
		if err != nil {
			return err
		}
		if _, err := tx.InsertMessage(&store.Message{
			Peer: to, MsgID: mid, Outgoing: true, FromSeq: seq, Lamport: lam,
			SentAt: time.Now().UnixMilli(), Status: store.StatusQueued, Body: []byte(text),
		}); err != nil {
			return err
		}
		return core.Outbox().EnqueueTx(tx, to, envelope.Envelope{
			MsgID: mid, Type: envelope.TypeChat, FromSeq: seq, LamportTS: lam,
			Payload: []byte(text),
		})
	})
	if err != nil {
		t.Fatalf("SendChat: %v", err)
	}
	core.Outbox().Flush(to)
	return mid
}

// RecordChat регистрирует на ядре обработчик TYPE_CHAT, прототипирующий
// путь приёма пакета chat: лампорт и история в одной транзакции с дедупом.
// before, если задан, зовётся внутри транзакции до записи — точка инъекции
// ошибок для crash-сценариев.
func RecordChat(core *app.Core, before func() error) {
	core.Outbox().Handle(envelope.TypeChat, func(tx *store.Tx, from peer.ID, env *envelope.Envelope) error {
		if before != nil {
			if err := before(); err != nil {
				return err
			}
		}
		if err := tx.LamportObserve(env.LamportTS); err != nil {
			return err
		}
		_, err := tx.InsertMessage(&store.Message{
			Peer: from, MsgID: env.MsgID, Outgoing: false, FromSeq: env.FromSeq,
			Lamport: env.LamportTS, SentAt: time.Now().UnixMilli(),
			Status: store.StatusDelivered, Body: env.Payload,
		})
		return err
	})
}

// WaitMessageStatus ждёт, пока исходящее сообщение на узле n дорастёт до
// статуса want.
func WaitMessageStatus(t *testing.T, n *Node, p peer.ID, mid envelope.MsgID, want store.Status, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		synctest.Wait()
		core := n.Core()
		if core != nil {
			m, ok, err := core.Store().GetMessage(context.Background(), p, mid, true)
			if err != nil {
				t.Fatalf("GetMessage: %v", err)
			}
			if ok && m.Status >= want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("статус %v не достигнут за %v", want, timeout)
		}
		time.Sleep(time.Second)
	}
}

// WaitInboundMessage ждёт появления входящего сообщения в истории узла n.
func WaitInboundMessage(t *testing.T, n *Node, from peer.ID, mid envelope.MsgID, timeout time.Duration) store.Message {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		synctest.Wait()
		core := n.Core()
		if core != nil {
			m, ok, err := core.Store().GetMessage(context.Background(), from, mid, false)
			if err != nil {
				t.Fatalf("GetMessage: %v", err)
			}
			if ok {
				return m
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("входящее не появилось за %v", timeout)
		}
		time.Sleep(time.Second)
	}
}

// PeerID — peer.ID узла (конверсия из node.ID).
func (n *Node) PeerID() peer.ID { return peer.ID(n.id) }
