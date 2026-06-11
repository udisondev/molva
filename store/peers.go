package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
)

// Методы знакомства/блокировки и очереди текстов до сессии.

// PeerPut создаёт запись о пире или меняет её состояние.
func (t *Tx) PeerPut(p peer.ID, s PeerState, nowMs int64) error {
	_, err := t.tx.ExecContext(t.ctx,
		`INSERT INTO peers (peer, state, created_at, updated_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT (peer) DO UPDATE SET state = excluded.state, updated_at = excluded.updated_at`,
		p[:], int(s), nowMs, nowMs)
	if err != nil {
		return fmt.Errorf("store: peer put: %w", err)
	}
	return nil
}

// PeerDelete стирает запись (reject, unblock → незнакомец).
func (t *Tx) PeerDelete(p peer.ID) error {
	if _, err := t.tx.ExecContext(t.ctx, `DELETE FROM peers WHERE peer = ?`, p[:]); err != nil {
		return fmt.Errorf("store: peer delete: %w", err)
	}
	return nil
}

// PeerGet — состояние и алиас пира внутри транзакции.
func (t *Tx) PeerGet(p peer.ID) (PeerInfo, bool, error) {
	row := t.tx.QueryRowContext(t.ctx,
		`SELECT peer, state, alias_ct, created_at, updated_at FROM peers WHERE peer = ?`, p[:])
	return scanPeer(row, t.box)
}

// PeerAliasSet шифрует и сохраняет локальный алиас пира.
func (t *Tx) PeerAliasSet(p peer.ID, alias string, nowMs int64) error {
	var aliasCt any
	if alias != "" {
		ct, err := t.box.seal([]byte(alias), aadAlias(p))
		if err != nil {
			return err
		}
		aliasCt = ct
	}
	res, err := t.tx.ExecContext(t.ctx,
		`UPDATE peers SET alias_ct = ?, updated_at = ? WHERE peer = ?`, aliasCt, nowMs, p[:])
	if err != nil {
		return fmt.Errorf("store: алиас: %w", err)
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return fmt.Errorf("store: алиас: пир неизвестен")
	}
	return nil
}

// PendingChatAdd ставит msg_id в очередь ожидания сессии.
func (t *Tx) PendingChatAdd(p peer.ID, mid envelope.MsgID, nowMs int64) error {
	_, err := t.tx.ExecContext(t.ctx,
		`INSERT INTO pending_chat (peer, msg_id, queued_at) VALUES (?, ?, ?)
		 ON CONFLICT (peer, msg_id) DO NOTHING`, p[:], mid[:], nowMs)
	if err != nil {
		return fmt.Errorf("store: pending chat: %w", err)
	}
	return nil
}

// PendingChatTake забирает (и удаляет) всю очередь ожидания по пиру в
// порядке постановки.
func (t *Tx) PendingChatTake(p peer.ID) ([]envelope.MsgID, error) {
	rows, err := t.tx.QueryContext(t.ctx,
		`DELETE FROM pending_chat WHERE peer = ? RETURNING msg_id, queued_at`, p[:])
	if err != nil {
		return nil, fmt.Errorf("store: pending chat take: %w", err)
	}
	defer rows.Close()
	type entry struct {
		mid envelope.MsgID
		at  int64
	}
	var entries []entry
	for rows.Next() {
		var (
			mb []byte
			at int64
		)
		if err := rows.Scan(&mb, &at); err != nil {
			return nil, fmt.Errorf("store: pending chat take: %w", err)
		}
		var e entry
		copy(e.mid[:], mb)
		e.at = at
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: pending chat take: %w", err)
	}
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0 && entries[j].at < entries[j-1].at; j-- {
			entries[j], entries[j-1] = entries[j-1], entries[j]
		}
	}
	out := make([]envelope.MsgID, len(entries))
	for i, e := range entries {
		out[i] = e.mid
	}
	return out, nil
}

// GetMessage — сообщение по ключу истории внутри транзакции.
func (t *Tx) GetMessage(p peer.ID, mid envelope.MsgID, outgoing bool) (Message, bool, error) {
	row := t.tx.QueryRowContext(t.ctx,
		`SELECT peer, msg_id, outgoing, from_seq, lamport, sent_at, status, deleted, body_ct, sender
		 FROM messages WHERE peer = ? AND outgoing = ? AND msg_id = ?`,
		p[:], boolInt(outgoing), mid[:])
	m, err := scanMessage(row, t.box)
	if errors.Is(err, sql.ErrNoRows) {
		return Message{}, false, nil
	}
	if err != nil {
		return Message{}, false, err
	}
	return m, true, nil
}

// PeerGet — состояние и алиас пира.
func (d *DB) PeerGet(ctx context.Context, p peer.ID) (PeerInfo, bool, error) {
	row := d.sql.QueryRowContext(ctx,
		`SELECT peer, state, alias_ct, created_at, updated_at FROM peers WHERE peer = ?`, p[:])
	return scanPeer(row, d.box)
}

// PeerList — все записи о пирах.
func (d *DB) PeerList(ctx context.Context) ([]PeerInfo, error) {
	rows, err := d.sql.QueryContext(ctx,
		`SELECT peer, state, alias_ct, created_at, updated_at FROM peers ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("store: peers: %w", err)
	}
	defer rows.Close()
	var out []PeerInfo
	for rows.Next() {
		info, ok, err := scanPeer(rows, d.box)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, info)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: peers: %w", err)
	}
	return out, nil
}

func scanPeer(s scanner, bx box) (PeerInfo, bool, error) {
	var (
		info    PeerInfo
		pb      []byte
		state   int
		aliasCt []byte
	)
	err := s.Scan(&pb, &state, &aliasCt, &info.CreatedAt, &info.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return PeerInfo{}, false, nil
	}
	if err != nil {
		return PeerInfo{}, false, fmt.Errorf("store: scan peer: %w", err)
	}
	copy(info.Peer[:], pb)
	info.State = PeerState(state)
	if aliasCt != nil {
		alias, err := bx.open(aliasCt, aadAlias(info.Peer))
		if err != nil {
			return PeerInfo{}, false, err
		}
		info.Alias = string(alias)
	}
	return info, true, nil
}

func aadAlias(p peer.ID) []byte {
	out := make([]byte, 0, len("peers.alias")+peer.IDLen)
	out = append(out, "peers.alias"...)
	out = append(out, p[:]...)
	return out
}
