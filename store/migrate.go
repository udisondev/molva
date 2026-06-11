package store

import (
	"context"
	"database/sql"
	"fmt"
)

// migrations — последовательные версии схемы; PRAGMA user_version хранит
// номер применённой. Менять задним числом нельзя — только добавлять.
var migrations = []string{
	// v1: сообщения, outbox, дедуп-окно, счётчики, служебные метаданные.
	`
CREATE TABLE meta (
  k TEXT PRIMARY KEY,
  v BLOB NOT NULL
) WITHOUT ROWID;

CREATE TABLE messages (
  id       INTEGER PRIMARY KEY,
  peer     BLOB    NOT NULL,
  msg_id   BLOB    NOT NULL,
  outgoing INTEGER NOT NULL,
  from_seq INTEGER NOT NULL DEFAULT 0,
  lamport  INTEGER NOT NULL DEFAULT 0,
  sent_at  INTEGER NOT NULL,
  status   INTEGER NOT NULL,
  deleted  INTEGER NOT NULL DEFAULT 0,
  body_ct  BLOB,
  UNIQUE (peer, outgoing, msg_id)
);
CREATE INDEX messages_peer_order ON messages (peer, lamport, id);

CREATE TABLE outbox (
  id         INTEGER PRIMARY KEY,
  peer       BLOB    NOT NULL,
  msg_id     BLOB    NOT NULL,
  frame_ct   BLOB    NOT NULL,
  attempts   INTEGER NOT NULL DEFAULT 0,
  next_at    INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  UNIQUE (peer, msg_id)
);
CREATE INDEX outbox_due ON outbox (next_at);

CREATE TABLE dedup (
  peer    BLOB    NOT NULL,
  msg_id  BLOB    NOT NULL,
  seen_at INTEGER NOT NULL,
  PRIMARY KEY (peer, msg_id)
) WITHOUT ROWID;
CREATE INDEX dedup_age ON dedup (peer, seen_at);

CREATE TABLE counters (
  scope TEXT PRIMARY KEY,
  value INTEGER NOT NULL
) WITHOUT ROWID;
`,
	// v2: состояния ratchet-сессий и незавершённые рукопожатия.
	`
CREATE TABLE sessions (
  peer       BLOB PRIMARY KEY,
  state_ct   BLOB    NOT NULL,
  updated_at INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE handshakes (
  peer       BLOB PRIMARY KEY,
  hs_ct      BLOB    NOT NULL,
  sid        BLOB    NOT NULL,
  created_at INTEGER NOT NULL
) WITHOUT ROWID;
`,
	// v3: контакты (знакомство/блокировка/алиасы) и очередь текстов,
	// ждущих установления сессии.
	`
CREATE TABLE peers (
  peer       BLOB PRIMARY KEY,
  state      INTEGER NOT NULL,
  alias_ct   BLOB,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE pending_chat (
  peer      BLOB    NOT NULL,
  msg_id    BLOB    NOT NULL,
  queued_at INTEGER NOT NULL,
  PRIMARY KEY (peer, msg_id)
) WITHOUT ROWID;
`,
	// v4: передачи файлов (манифесты, битмапы приёма, резюм).
	`
CREATE TABLE files (
  file_id     BLOB PRIMARY KEY,
  peer        BLOB    NOT NULL,
  outgoing    INTEGER NOT NULL,
  manifest_ct BLOB    NOT NULL,
  path_ct     BLOB    NOT NULL,
  bitmap      BLOB    NOT NULL,
  done        INTEGER NOT NULL DEFAULT 0,
  created_at  INTEGER NOT NULL,
  updated_at  INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX files_peer ON files (peer);
`,
}

// migrate доводит схему до актуальной версии; каждая миграция — своя
// транзакция, рестарт посреди серии безопасен.
func migrate(ctx context.Context, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("store: user_version: %w", err)
	}
	for v := version; v < len(migrations); v++ {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("store: миграция %d: %w", v+1, err)
		}
		if _, err := tx.ExecContext(ctx, migrations[v]); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("store: миграция %d: %w", v+1, err)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", v+1)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("store: миграция %d: %w", v+1, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("store: миграция %d: %w", v+1, err)
		}
	}
	return nil
}
