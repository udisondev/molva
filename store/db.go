// Package store — SQLite-хранилище molva: история, outbox, дедуп-окно,
// счётчики (дальше: ratchet-состояния, контакты, группы, манифесты).
// Драйвер pure-Go (modernc.org/sqlite) — сборка sidecar'а без cgo под три ОС.
//
// Дисциплина корректности: «обработал входящее сообщение» — одна транзакция
// {продвинуть крипто-состояние, записать сообщение, отметить дедуп}; для
// этого вся запись идёт через DB.Tx. Контент-поля (тела сообщений, кадры
// outbox) шифруются ключом из master-seed; метаданные открыты — по ним
// индексы, их защита — FDE десктопа.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
	_ "modernc.org/sqlite"
)

// DB — открытая база одного узла. Одно соединение: нагрузка мессенджера
// лёгкая, а единственный писатель снимает вопросы SQLITE_BUSY.
type DB struct {
	sql *sql.DB
	box box
}

// Open открывает (создавая при необходимости) базу по пути path с ключом
// контент-полей key. Неподходящий ключ — ErrWrongKey сразу, по канарейке.
func Open(path string, key [32]byte) (*DB, error) {
	bx, err := newBox(key)
	if err != nil {
		return nil, err
	}
	dsn := "file:" + path +
		"?_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(FULL)" +
		"&_pragma=busy_timeout(5000)" +
		"&_pragma=foreign_keys(ON)"
	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	sqlDB.SetMaxOpenConns(1)
	ctx := context.Background()
	if err := migrate(ctx, sqlDB); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	d := &DB{sql: sqlDB, box: bx}
	if err := d.checkCanary(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	return d, nil
}

// checkCanary проверяет ключ по контрольной записи; в свежей базе — пишет её.
func (d *DB) checkCanary(ctx context.Context) error {
	var blob []byte
	err := d.sql.QueryRowContext(ctx, `SELECT v FROM meta WHERE k = 'canary'`).Scan(&blob)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		sealed, err := d.box.seal([]byte("molva"), aadMeta("canary"))
		if err != nil {
			return err
		}
		_, err = d.sql.ExecContext(ctx, `INSERT INTO meta (k, v) VALUES ('canary', ?)`, sealed)
		if err != nil {
			return fmt.Errorf("store: канарейка: %w", err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("store: канарейка: %w", err)
	default:
		_, err := d.box.open(blob, aadMeta("canary"))
		return err
	}
}

// Close закрывает базу.
func (d *DB) Close() error { return d.sql.Close() }

// Tx выполняет fn в одной транзакции; ошибка fn откатывает всё.
func (d *DB) Tx(ctx context.Context, fn func(tx *Tx) error) error {
	sqlTx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin: %w", err)
	}
	t := &Tx{ctx: ctx, tx: sqlTx, box: d.box}
	if err := fn(t); err != nil {
		_ = sqlTx.Rollback()
		return err
	}
	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("store: commit: %w", err)
	}
	return nil
}

// ListMessages — последние limit сообщений диалога в порядке отображения
// (lamport, затем id). limit <= 0 — все.
func (d *DB) ListMessages(ctx context.Context, p peer.ID, limit int) ([]Message, error) {
	q := `SELECT peer, msg_id, outgoing, from_seq, lamport, sent_at, status, deleted, body_ct
	      FROM messages WHERE peer = ? ORDER BY lamport DESC, id DESC`
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := d.sql.QueryContext(ctx, q, p[:])
	if err != nil {
		return nil, fmt.Errorf("store: messages: %w", err)
	}
	defer rows.Close()
	var out []Message
	for rows.Next() {
		m, err := scanMessage(rows, d.box)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: messages: %w", err)
	}
	// Возврат в хронологии (читали с конца).
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

// GetMessage — одно сообщение по ключу истории.
func (d *DB) GetMessage(ctx context.Context, p peer.ID, mid envelope.MsgID, outgoing bool) (Message, bool, error) {
	row := d.sql.QueryRowContext(ctx,
		`SELECT peer, msg_id, outgoing, from_seq, lamport, sent_at, status, deleted, body_ct
		 FROM messages WHERE peer = ? AND outgoing = ? AND msg_id = ?`,
		p[:], boolInt(outgoing), mid[:])
	m, err := scanMessage(row, d.box)
	if errors.Is(err, sql.ErrNoRows) {
		return Message{}, false, nil
	}
	if err != nil {
		return Message{}, false, err
	}
	return m, true, nil
}

// OutboxDue — элементы, чья пора пришла (next_at <= nowMs), старые сначала.
func (d *DB) OutboxDue(ctx context.Context, nowMs int64, limit int) ([]OutboxItem, error) {
	rows, err := d.sql.QueryContext(ctx,
		`SELECT id, peer, msg_id, frame_ct, attempts, next_at FROM outbox
		 WHERE next_at <= ? ORDER BY next_at, id LIMIT ?`, nowMs, limit)
	if err != nil {
		return nil, fmt.Errorf("store: outbox due: %w", err)
	}
	defer rows.Close()
	var out []OutboxItem
	for rows.Next() {
		var (
			it      OutboxItem
			pb, mb  []byte
			frameCt []byte
		)
		if err := rows.Scan(&it.ID, &pb, &mb, &frameCt, &it.Attempts, &it.NextAt); err != nil {
			return nil, fmt.Errorf("store: outbox due: %w", err)
		}
		copy(it.Peer[:], pb)
		copy(it.MsgID[:], mb)
		frame, err := d.box.open(frameCt, aadOutbox(it.Peer, it.MsgID))
		if err != nil {
			return nil, err
		}
		it.Frame = frame
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: outbox due: %w", err)
	}
	return out, nil
}

// OutboxNearest — ближайший next_at очереди; false — очередь пуста.
func (d *DB) OutboxNearest(ctx context.Context) (int64, bool, error) {
	var at sql.NullInt64
	err := d.sql.QueryRowContext(ctx, `SELECT MIN(next_at) FROM outbox`).Scan(&at)
	if err != nil {
		return 0, false, fmt.Errorf("store: outbox nearest: %w", err)
	}
	return at.Int64, at.Valid, nil
}

// OutboxPending — сколько конвертов ждёт доставки пиру.
func (d *DB) OutboxPending(ctx context.Context, p peer.ID) (int, error) {
	var n int
	err := d.sql.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox WHERE peer = ?`, p[:]).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("store: outbox pending: %w", err)
	}
	return n, nil
}

// DedupSeen — встречался ли msg_id от этого пира в окне дедупликации.
func (d *DB) DedupSeen(ctx context.Context, p peer.ID, mid envelope.MsgID) (bool, error) {
	var one int
	err := d.sql.QueryRowContext(ctx,
		`SELECT 1 FROM dedup WHERE peer = ? AND msg_id = ?`, p[:], mid[:]).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: dedup: %w", err)
	}
	return true, nil
}

// scanner покрывает *sql.Row и *sql.Rows.
type scanner interface{ Scan(dest ...any) error }

func scanMessage(s scanner, bx box) (Message, error) {
	var (
		m        Message
		pb, mb   []byte
		outgoing int
		status   int
		deleted  int
		bodyCt   []byte
	)
	if err := s.Scan(&pb, &mb, &outgoing, &m.FromSeq, &m.Lamport, &m.SentAt, &status, &deleted, &bodyCt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Message{}, err
		}
		return Message{}, fmt.Errorf("store: scan message: %w", err)
	}
	copy(m.Peer[:], pb)
	copy(m.MsgID[:], mb)
	m.Outgoing = outgoing != 0
	m.Status = Status(status)
	m.Deleted = deleted != 0
	if bodyCt != nil {
		body, err := bx.open(bodyCt, aadMessage(m.Peer, m.MsgID, m.Outgoing))
		if err != nil {
			return Message{}, err
		}
		m.Body = body
	}
	return m, nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
