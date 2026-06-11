package store

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/udisondev/molva/peer"
)

// Методы персистентности крипто-состояний. Дисциплина использования:
// загрузка, продвижение и сохранение состояния сессии происходят в ОДНОЙ
// транзакции с эффектом сообщения — поэтому всё живёт на Tx.

// SessionGet читает сериализованное состояние сессии с пиром.
func (t *Tx) SessionGet(p peer.ID) ([]byte, bool, error) {
	var ct []byte
	err := t.tx.QueryRowContext(t.ctx,
		`SELECT state_ct FROM sessions WHERE peer = ?`, p[:]).Scan(&ct)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("store: сессия: %w", err)
	}
	state, err := t.box.open(ct, aadSession(p))
	if err != nil {
		return nil, false, err
	}
	return state, true, nil
}

// SessionPut сохраняет состояние сессии (создавая или замещая).
func (t *Tx) SessionPut(p peer.ID, state []byte, nowMs int64) error {
	ct, err := t.box.seal(state, aadSession(p))
	if err != nil {
		return err
	}
	_, err = t.tx.ExecContext(t.ctx,
		`INSERT INTO sessions (peer, state_ct, updated_at) VALUES (?, ?, ?)
		 ON CONFLICT (peer) DO UPDATE SET state_ct = excluded.state_ct, updated_at = excluded.updated_at`,
		p[:], ct, nowMs)
	if err != nil {
		return fmt.Errorf("store: сессия: %w", err)
	}
	return nil
}

// SessionDelete удаляет сессию (re-handshake, блокировка).
func (t *Tx) SessionDelete(p peer.ID) error {
	if _, err := t.tx.ExecContext(t.ctx, `DELETE FROM sessions WHERE peer = ?`, p[:]); err != nil {
		return fmt.Errorf("store: сессия: %w", err)
	}
	return nil
}

// HandshakeGet читает незавершённое рукопожатие с пиром.
func (t *Tx) HandshakeGet(p peer.ID) ([]byte, bool, error) {
	var ct []byte
	err := t.tx.QueryRowContext(t.ctx,
		`SELECT hs_ct FROM handshakes WHERE peer = ?`, p[:]).Scan(&ct)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("store: рукопожатие: %w", err)
	}
	blob, err := t.box.open(ct, aadHandshake(p))
	if err != nil {
		return nil, false, err
	}
	return blob, true, nil
}

// HandshakePut сохраняет незавершённое рукопожатие (замещая прежнее).
func (t *Tx) HandshakePut(p peer.ID, blob []byte, sid []byte, nowMs int64) error {
	ct, err := t.box.seal(blob, aadHandshake(p))
	if err != nil {
		return err
	}
	_, err = t.tx.ExecContext(t.ctx,
		`INSERT INTO handshakes (peer, hs_ct, sid, created_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT (peer) DO UPDATE SET hs_ct = excluded.hs_ct, sid = excluded.sid, created_at = excluded.created_at`,
		p[:], ct, sid, nowMs)
	if err != nil {
		return fmt.Errorf("store: рукопожатие: %w", err)
	}
	return nil
}

// HandshakeDelete удаляет завершённое или отменённое рукопожатие.
func (t *Tx) HandshakeDelete(p peer.ID) error {
	if _, err := t.tx.ExecContext(t.ctx, `DELETE FROM handshakes WHERE peer = ?`, p[:]); err != nil {
		return fmt.Errorf("store: рукопожатие: %w", err)
	}
	return nil
}

func aadSession(p peer.ID) []byte {
	out := make([]byte, 0, len("sessions.state")+peer.IDLen)
	out = append(out, "sessions.state"...)
	out = append(out, p[:]...)
	return out
}

func aadHandshake(p peer.ID) []byte {
	out := make([]byte, 0, len("handshakes.hs")+peer.IDLen)
	out = append(out, "handshakes.hs"...)
	out = append(out, p[:]...)
	return out
}
