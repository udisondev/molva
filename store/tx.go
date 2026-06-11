package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
)

// Tx — открытая транзакция; все мутации БД идут через её методы, чтобы
// эффект сообщения и крипто-состояние коммитились атомарно.
type Tx struct {
	ctx   context.Context
	tx    *sql.Tx
	box   box
	after []func()
}

// AfterCommit откладывает fn до успешного коммита: события и обновления
// кэшей не должны опережать долговечность (откат не должен их отзывать).
func (t *Tx) AfterCommit(fn func()) { t.after = append(t.after, fn) }

// InsertMessage пишет запись истории; тело шифруется на месте. Повторная
// вставка того же (peer, направление, msg_id) — не ошибка, а false:
// обработчики обязаны быть идемпотентными (пере-доставка после вытеснения
// дедуп-окна не должна ни падать, ни дублировать историю).
func (t *Tx) InsertMessage(m *Message) (bool, error) {
	var bodyCt any
	if m.Body != nil && !m.Deleted {
		ct, err := t.box.seal(m.Body, aadMessage(m.Peer, m.MsgID, m.Outgoing))
		if err != nil {
			return false, err
		}
		bodyCt = ct
	}
	res, err := t.tx.ExecContext(t.ctx,
		`INSERT INTO messages (peer, msg_id, outgoing, from_seq, lamport, sent_at, status, deleted, body_ct, sender)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (peer, outgoing, msg_id) DO NOTHING`,
		m.Peer[:], m.MsgID[:], boolInt(m.Outgoing), m.FromSeq, m.Lamport, m.SentAt,
		int(m.Status), boolInt(m.Deleted), bodyCt, m.Sender)
	if err != nil {
		return false, fmt.Errorf("store: insert message: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: insert message: %w", err)
	}
	return n > 0, nil
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

// lamportCeil — потолок принимаемой лампортовой метки. Метка приходит из
// недоверенного конверта: без потолка враждебный пир одной гигантской
// меткой переполнил бы знаковый INTEGER счётчика (через MAX(...)+1 SQLite
// уехал бы во float) и навсегда сломал отправку. 2^53 хватит на вечность
// и безопасно далеко от обеих границ (int64 и точности float64).
const lamportCeil = uint64(1) << 53

// LamportObserve продвигает лампортовы часы по принятой метке:
// value = max(value, min(remote, потолок)) + 1.
func (t *Tx) LamportObserve(remote uint64) error {
	remote = min(remote, lamportCeil)
	_, err := t.tx.ExecContext(t.ctx,
		`INSERT INTO counters (scope, value) VALUES ('lamport', ? + 1)
		 ON CONFLICT (scope) DO UPDATE SET value = MAX(value, ?) + 1`,
		remote, remote)
	if err != nil {
		return fmt.Errorf("store: lamport: %w", err)
	}
	return nil
}

// DedupInsert отмечает msg_id в окне дедупликации; false — уже встречался
// (запись при этом освежается: активные пере-доставки продлевают окно,
// чтобы ретраи пира не пережили его и не воскресили дубль).
func (t *Tx) DedupInsert(p peer.ID, mid envelope.MsgID, nowMs int64) (bool, error) {
	var one int
	err := t.tx.QueryRowContext(t.ctx,
		`SELECT 1 FROM dedup WHERE peer = ? AND msg_id = ?`, p[:], mid[:]).Scan(&one)
	switch {
	case err == nil:
		if _, err := t.tx.ExecContext(t.ctx,
			`UPDATE dedup SET seen_at = ? WHERE peer = ? AND msg_id = ?`,
			nowMs, p[:], mid[:]); err != nil {
			return false, fmt.Errorf("store: dedup refresh: %w", err)
		}
		return false, nil
	case errors.Is(err, sql.ErrNoRows):
		if _, err := t.tx.ExecContext(t.ctx,
			`INSERT INTO dedup (peer, msg_id, seen_at) VALUES (?, ?, ?)`,
			p[:], mid[:], nowMs); err != nil {
			return false, fmt.Errorf("store: dedup insert: %w", err)
		}
		return true, nil
	default:
		return false, fmt.Errorf("store: dedup insert: %w", err)
	}
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

// OutboxDeleteByID снимает ряд по внутреннему id (карантин порчи).
func (t *Tx) OutboxDeleteByID(id int64) error {
	if _, err := t.tx.ExecContext(t.ctx, `DELETE FROM outbox WHERE id = ?`, id); err != nil {
		return fmt.Errorf("store: outbox delete: %w", err)
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
