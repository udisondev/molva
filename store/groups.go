package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/udisondev/molva/peer"
)

// Методы персистентности групп, sender keys и sealed-очереди.

// GroupPut создаёт или обновляет группу.
func (t *Tx) GroupPut(g *GroupRec) error {
	nameCt, err := t.box.seal([]byte(g.Name), aadGroupName(g.GroupID))
	if err != nil {
		return err
	}
	memCt, err := t.box.seal(g.Membership, aadGroupMembership(g.GroupID))
	if err != nil {
		return err
	}
	_, err = t.tx.ExecContext(t.ctx,
		`INSERT INTO groups (group_id, name_ct, admin_pub, version, membership_ct, left, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (group_id) DO UPDATE SET
		   name_ct = excluded.name_ct, version = excluded.version,
		   membership_ct = excluded.membership_ct, left = excluded.left,
		   updated_at = excluded.updated_at`,
		g.GroupID[:], nameCt, g.AdminPub[:], g.Version, memCt, boolInt(g.Left),
		g.CreatedAt, g.UpdatedAt)
	if err != nil {
		return fmt.Errorf("store: group put: %w", err)
	}
	return nil
}

// GroupGet — группа по идентификатору (внутри транзакции).
func (t *Tx) GroupGet(gid [32]byte) (GroupRec, bool, error) {
	row := t.tx.QueryRowContext(t.ctx,
		`SELECT group_id, name_ct, admin_pub, version, membership_ct, left, created_at, updated_at
		 FROM groups WHERE group_id = ?`, gid[:])
	return scanGroup(row, t.box)
}

// GroupMembersSet замещает состав группы.
func (t *Tx) GroupMembersSet(gid [32]byte, members []peer.ID) error {
	if _, err := t.tx.ExecContext(t.ctx, `DELETE FROM group_members WHERE group_id = ?`, gid[:]); err != nil {
		return fmt.Errorf("store: members: %w", err)
	}
	for _, m := range members {
		if _, err := t.tx.ExecContext(t.ctx,
			`INSERT INTO group_members (group_id, member) VALUES (?, ?)`, gid[:], m[:]); err != nil {
			return fmt.Errorf("store: members: %w", err)
		}
	}
	return nil
}

// GroupMembers — состав группы (внутри транзакции).
func (t *Tx) GroupMembers(gid [32]byte) ([]peer.ID, error) {
	rows, err := t.tx.QueryContext(t.ctx,
		`SELECT member FROM group_members WHERE group_id = ? ORDER BY member`, gid[:])
	if err != nil {
		return nil, fmt.Errorf("store: members: %w", err)
	}
	defer rows.Close()
	var out []peer.ID
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, fmt.Errorf("store: members: %w", err)
		}
		var p peer.ID
		copy(p[:], b)
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: members: %w", err)
	}
	return out, nil
}

// GroupIsMember — состоит ли пир в группе (внутри транзакции).
func (t *Tx) GroupIsMember(gid [32]byte, p peer.ID) (bool, error) {
	var one int
	err := t.tx.QueryRowContext(t.ctx,
		`SELECT 1 FROM group_members WHERE group_id = ? AND member = ?`, gid[:], p[:]).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: member: %w", err)
	}
	return true, nil
}

// SenderKeyPut сохраняет состояние ключа участника (свой Sender или
// чужой Receiver) шифрованно.
func (t *Tx) SenderKeyPut(gid [32]byte, member peer.ID, state []byte, nowMs int64) error {
	ct, err := t.box.seal(state, aadSenderKey(gid, member))
	if err != nil {
		return err
	}
	_, err = t.tx.ExecContext(t.ctx,
		`INSERT INTO sender_keys (group_id, member, state_ct, updated_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT (group_id, member) DO UPDATE SET state_ct = excluded.state_ct, updated_at = excluded.updated_at`,
		gid[:], member[:], ct, nowMs)
	if err != nil {
		return fmt.Errorf("store: sender key: %w", err)
	}
	return nil
}

// SenderKeyGet — состояние ключа участника.
func (t *Tx) SenderKeyGet(gid [32]byte, member peer.ID) ([]byte, bool, error) {
	var ct []byte
	err := t.tx.QueryRowContext(t.ctx,
		`SELECT state_ct FROM sender_keys WHERE group_id = ? AND member = ?`,
		gid[:], member[:]).Scan(&ct)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("store: sender key: %w", err)
	}
	state, err := t.box.open(ct, aadSenderKey(gid, member))
	if err != nil {
		return nil, false, err
	}
	return state, true, nil
}

// SenderKeyDelete стирает ключ участника (удалён из группы).
func (t *Tx) SenderKeyDelete(gid [32]byte, member peer.ID) error {
	_, err := t.tx.ExecContext(t.ctx,
		`DELETE FROM sender_keys WHERE group_id = ? AND member = ?`, gid[:], member[:])
	if err != nil {
		return fmt.Errorf("store: sender key: %w", err)
	}
	return nil
}

// SealedOutboxAdd откладывает sealed-рассылку до появления сессии.
func (t *Tx) SealedOutboxAdd(p peer.ID, envType uint8, payload []byte, nowMs int64) error {
	ct, err := t.box.seal(payload, aadSealedOutbox(p, envType))
	if err != nil {
		return err
	}
	_, err = t.tx.ExecContext(t.ctx,
		`INSERT INTO sealed_outbox (peer, env_type, payload_ct, created_at) VALUES (?, ?, ?, ?)`,
		p[:], int(envType), ct, nowMs)
	if err != nil {
		return fmt.Errorf("store: sealed outbox: %w", err)
	}
	return nil
}

// SealedOutboxDelete снимает отправленную рассылку.
func (t *Tx) SealedOutboxDelete(id int64) error {
	if _, err := t.tx.ExecContext(t.ctx, `DELETE FROM sealed_outbox WHERE id = ?`, id); err != nil {
		return fmt.Errorf("store: sealed outbox: %w", err)
	}
	return nil
}

// SealedOutboxPurgePeer очищает отложенные рассылки в сторону пира
// (блокировка): иначе групповые ключи/документы ретраятся к нему вечно.
func (t *Tx) SealedOutboxPurgePeer(p peer.ID) error {
	if _, err := t.tx.ExecContext(t.ctx, `DELETE FROM sealed_outbox WHERE peer = ?`, p[:]); err != nil {
		return fmt.Errorf("store: sealed outbox purge: %w", err)
	}
	return nil
}

// GroupList — все группы.
func (d *DB) GroupList(ctx context.Context) ([]GroupRec, error) {
	rows, err := d.sql.QueryContext(ctx,
		`SELECT group_id, name_ct, admin_pub, version, membership_ct, left, created_at, updated_at
		 FROM groups ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("store: groups: %w", err)
	}
	defer rows.Close()
	var out []GroupRec
	for rows.Next() {
		g, ok, err := scanGroup(rows, d.box)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, g)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: groups: %w", err)
	}
	return out, nil
}

// SealedOutboxList — отложенные рассылки, старые сначала.
func (d *DB) SealedOutboxList(ctx context.Context, limit int) ([]SealedItem, error) {
	rows, err := d.sql.QueryContext(ctx,
		`SELECT id, peer, env_type, payload_ct FROM sealed_outbox ORDER BY id LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: sealed outbox: %w", err)
	}
	defer rows.Close()
	var out []SealedItem
	for rows.Next() {
		var (
			it      SealedItem
			pb, ct  []byte
			envType int
		)
		if err := rows.Scan(&it.ID, &pb, &envType, &ct); err != nil {
			return nil, fmt.Errorf("store: sealed outbox: %w", err)
		}
		copy(it.Peer[:], pb)
		it.EnvType = uint8(envType)
		payload, err := d.box.open(ct, aadSealedOutbox(peer.ID(it.Peer), it.EnvType))
		if err != nil {
			return nil, err
		}
		it.Payload = payload
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: sealed outbox: %w", err)
	}
	return out, nil
}

func scanGroup(s scanner, bx box) (GroupRec, bool, error) {
	var (
		g            GroupRec
		gidB, adminB []byte
		nameCt       []byte
		memCt        []byte
		left         int
	)
	err := s.Scan(&gidB, &nameCt, &adminB, &g.Version, &memCt, &left, &g.CreatedAt, &g.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return GroupRec{}, false, nil
	}
	if err != nil {
		return GroupRec{}, false, fmt.Errorf("store: scan group: %w", err)
	}
	copy(g.GroupID[:], gidB)
	copy(g.AdminPub[:], adminB)
	g.Left = left != 0
	if nameCt != nil {
		name, err := bx.open(nameCt, aadGroupName(g.GroupID))
		if err != nil {
			return GroupRec{}, false, err
		}
		g.Name = string(name)
	}
	mem, err := bx.open(memCt, aadGroupMembership(g.GroupID))
	if err != nil {
		return GroupRec{}, false, err
	}
	g.Membership = mem
	return g, true, nil
}

func aadGroupName(gid [32]byte) []byte {
	return append([]byte("groups.name"), gid[:]...)
}

func aadGroupMembership(gid [32]byte) []byte {
	return append([]byte("groups.membership"), gid[:]...)
}

func aadSenderKey(gid [32]byte, member peer.ID) []byte {
	out := append([]byte("sender_keys.state"), gid[:]...)
	return append(out, member[:]...)
}

func aadSealedOutbox(p peer.ID, envType uint8) []byte {
	out := append([]byte("sealed_outbox.payload"), p[:]...)
	return append(out, envType)
}
