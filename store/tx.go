package store

import (
	"database/sql"
	"context"
	"fmt"

	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
)

// Tx — открытая транзакция; все мутации БД идут через её методы, чтобы
// эффект сообщения и крипто-состояние коммитились атомарно.
type Tx struct {
	ctx context.Context
	tx  *sql.Tx
	box box
}

// InsertMessage пишет запись истории; тело шифруется на месте.
func (t *Tx) InsertMessage(m *Message) error {
	var bodyCt any
	if m.Body != nil && !m.Deleted {
		ct, err := t.box.seal(m.Body, aadMessage(m.Peer, m.MsgID, m.Outgoing))
		if err != nil {
			return err
		}
		bodyCt = ct
	}
	_, err := t.tx.ExecContext(t.ctx,
		`INSERT INTO messages (peer, msg_id, outgoing, from_seq, lamport, sent_at, status, deleted, body_ct)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.Peer[:], m.MsgID[:], boolInt(m.Outgoing), m.FromSeq, m.Lamport, m.SentAt,
		int(m.Status), boolInt(m.Deleted), bodyCt)
	if err != nil {
		return fmt.Errorf("store: insert message: %w", err)
	}
	return nil
}

// MessageStatusUp поднимает статус исходящего сообщения; понижения
// игнорируются (статусы монотонны), отсутствие строки — не ошибка
// (служебные конверты истории не имеют).
func (t *Tx) MessageStatusUp(p peer.ID, mid envelope.MsgID, s Status) error {
	_, err := t.tx.ExecContext(t.ctx,
		`UPDATE messages SET status = ? WHERE peer = ? AND outgoing = 1 AND msg_id = ? AND status < ?`,
		int(s), p[:], mid[:], int(s))
	if err != nil {
		return fmt.Errorf("store: статус: %w", err)
	}
	return nil
}

// DeleteMessageBody — локальное удаление: контент стирается, метаданные и
// дедуп-окно остаются — пере-доставка не воскресит удалённое.
func (t *Tx) DeleteMessageBody(p peer.ID, mid envelope.MsgID) error {
	_, err := t.tx.ExecContext(t.ctx,
		`UPDATE messages SET deleted = 1, body_ct = NULL WHERE peer = ? AND msg_id = ?`,
		p[:], mid[:])
	if err != nil {
		return fmt.Errorf("store: удаление: %w", err)
	}
	return nil
}

// NextSeq — следующий номер монотонного счётчика scope (например,
// per-peer from_seq исходящих).
func (t *Tx) NextSeq(scope string) (uint64, error) {
	var v uint64
	err := t.tx.QueryRowContext(t.ctx,
		`INSERT INTO counters (scope, value) VALUES (?, 1)
		 ON CONFLICT (scope) DO UPDATE SET value = value + 1
		 RETURNING value`, scope).Scan(&v)
	if err != nil {
		return 0, fmt.Errorf("store: счётчик %s: %w", scope, err)
	}
	return v, nil
}

// LamportNext — тик лампортовых часов для исходящего.
func (t *Tx) LamportNext() (uint64, error) { return t.NextSeq("lamport") }

// LamportObserve продвигает лампортовы часы по принятой метке:
// value = max(value, remote) + 1.
func (t *Tx) LamportObserve(remote uint64) error {
	_, err := t.tx.ExecContext(t.ctx,
		`INSERT INTO counters (scope, value) VALUES ('lamport', ? + 1)
		 ON CONFLICT (scope) DO UPDATE SET value = MAX(value, ?) + 1`,
		remote, remote)
	if err != nil {
		return fmt.Errorf("store: lamport: %w", err)
	}
	return nil
}

// DedupInsert отмечает msg_id в окне дедупликации; false — уже встречался.
func (t *Tx) DedupInsert(p peer.ID, mid envelope.MsgID, nowMs int64) (bool, error) {
	res, err := t.tx.ExecContext(t.ctx,
		`INSERT INTO dedup (peer, msg_id, seen_at) VALUES (?, ?, ?)
		 ON CONFLICT (peer, msg_id) DO NOTHING`,
		p[:], mid[:], nowMs)
	if err != nil {
		return false, fmt.Errorf("store: dedup insert: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: dedup insert: %w", err)
	}
	return n > 0, nil
}

// DedupPrune подрезает окно пира: по возрасту (cutoffMs) и по ёмкости
// (cap записей, старые вылетают первыми).
func (t *Tx) DedupPrune(p peer.ID, cutoffMs int64, cap int) error {
	if _, err := t.tx.ExecContext(t.ctx,
		`DELETE FROM dedup WHERE peer = ? AND seen_at < ?`, p[:], cutoffMs); err != nil {
		return fmt.Errorf("store: dedup prune: %w", err)
	}
	_, err := t.tx.ExecContext(t.ctx,
		`DELETE FROM dedup WHERE peer = ? AND (msg_id, seen_at) NOT IN (
		   SELECT msg_id, seen_at FROM dedup WHERE peer = ? ORDER BY seen_at DESC LIMIT ?)`,
		p[:], p[:], cap)
	if err != nil {
		return fmt.Errorf("store: dedup prune: %w", err)
	}
	return nil
}

// OutboxEnqueue ставит кадр конверта в персистентную очередь с немедленной
// первой попыткой.
func (t *Tx) OutboxEnqueue(p peer.ID, mid envelope.MsgID, frame []byte, nowMs int64) error {
	ct, err := t.box.seal(frame, aadOutbox(p, mid))
	if err != nil {
		return err
	}
	_, err = t.tx.ExecContext(t.ctx,
		`INSERT INTO outbox (peer, msg_id, frame_ct, attempts, next_at, created_at)
		 VALUES (?, ?, ?, 0, ?, ?)`,
		p[:], mid[:], ct, nowMs, nowMs)
	if err != nil {
		return fmt.Errorf("store: outbox enqueue: %w", err)
	}
	return nil
}

// OutboxSettle снимает конверт с очереди по ack'у; false — не было такого
// (повторный или чужой ack).
func (t *Tx) OutboxSettle(p peer.ID, mid envelope.MsgID) (bool, error) {
	res, err := t.tx.ExecContext(t.ctx,
		`DELETE FROM outbox WHERE peer = ? AND msg_id = ?`, p[:], mid[:])
	if err != nil {
		return false, fmt.Errorf("store: outbox settle: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: outbox settle: %w", err)
	}
	return n > 0, nil
}

// OutboxAttempt фиксирует попытку отправки и время следующей.
func (t *Tx) OutboxAttempt(id int64, attempts int, nextAtMs int64) error {
	_, err := t.tx.ExecContext(t.ctx,
		`UPDATE outbox SET attempts = ?, next_at = ? WHERE id = ?`, attempts, nextAtMs, id)
	if err != nil {
		return fmt.Errorf("store: outbox attempt: %w", err)
	}
	return nil
}

// OutboxKick сбрасывает backoff очереди пира — признак его присутствия
// делает ожидание бессмысленным.
func (t *Tx) OutboxKick(p peer.ID, nowMs int64) error {
	_, err := t.tx.ExecContext(t.ctx,
		`UPDATE outbox SET attempts = 0, next_at = ? WHERE peer = ? AND next_at > ?`,
		nowMs, p[:], nowMs)
	if err != nil {
		return fmt.Errorf("store: outbox kick: %w", err)
	}
	return nil
}

// OutboxPurgePeer очищает очередь в сторону пира (блокировка).
func (t *Tx) OutboxPurgePeer(p peer.ID) error {
	_, err := t.tx.ExecContext(t.ctx, `DELETE FROM outbox WHERE peer = ?`, p[:])
	if err != nil {
		return fmt.Errorf("store: outbox purge: %w", err)
	}
	return nil
}
