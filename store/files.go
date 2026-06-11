package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Методы персистентности передач файлов.

// FilePut создаёт или замещает запись передачи. Пустой битмап (исходящие)
// хранится пустым блобом.
func (t *Tx) FilePut(r *FileRec) error {
	if r.Bitmap == nil {
		r.Bitmap = []byte{}
	}
	manCt, err := t.box.seal(r.Manifest, aadFileManifest(r.FileID))
	if err != nil {
		return err
	}
	pathCt, err := t.box.seal([]byte(r.Path), aadFilePath(r.FileID))
	if err != nil {
		return err
	}
	_, err = t.tx.ExecContext(t.ctx,
		`INSERT INTO files (file_id, peer, outgoing, manifest_ct, path_ct, bitmap, done, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (file_id) DO UPDATE SET
		   manifest_ct = excluded.manifest_ct, path_ct = excluded.path_ct,
		   bitmap = excluded.bitmap, done = excluded.done, updated_at = excluded.updated_at`,
		r.FileID[:], r.Peer[:], boolInt(r.Outgoing), manCt, pathCt, r.Bitmap,
		boolInt(r.Done), r.CreatedAt, r.UpdatedAt)
	if err != nil {
		return fmt.Errorf("store: file put: %w", err)
	}
	return nil
}

// FileProgress обновляет битмап и флаг завершения.
func (t *Tx) FileProgress(fileID [16]byte, bitmap []byte, done bool, nowMs int64) error {
	_, err := t.tx.ExecContext(t.ctx,
		`UPDATE files SET bitmap = ?, done = ?, updated_at = ? WHERE file_id = ?`,
		bitmap, boolInt(done), nowMs, fileID[:])
	if err != nil {
		return fmt.Errorf("store: file progress: %w", err)
	}
	return nil
}

// FilePath обновляет локальный путь (после переименования .part).
func (t *Tx) FilePath(fileID [16]byte, path string, nowMs int64) error {
	pathCt, err := t.box.seal([]byte(path), aadFilePath(fileID))
	if err != nil {
		return err
	}
	_, err = t.tx.ExecContext(t.ctx,
		`UPDATE files SET path_ct = ?, updated_at = ? WHERE file_id = ?`, pathCt, nowMs, fileID[:])
	if err != nil {
		return fmt.Errorf("store: file path: %w", err)
	}
	return nil
}

// FileGet — запись передачи по идентификатору.
func (d *DB) FileGet(ctx context.Context, fileID [16]byte) (FileRec, bool, error) {
	row := d.sql.QueryRowContext(ctx,
		`SELECT file_id, peer, outgoing, manifest_ct, path_ct, bitmap, done, created_at, updated_at
		 FROM files WHERE file_id = ?`, fileID[:])
	return scanFile(row, d.box)
}

// FileListUnfinished — незавершённые приёмы (для резюма после рестарта).
func (d *DB) FileListUnfinished(ctx context.Context) ([]FileRec, error) {
	rows, err := d.sql.QueryContext(ctx,
		`SELECT file_id, peer, outgoing, manifest_ct, path_ct, bitmap, done, created_at, updated_at
		 FROM files WHERE done = 0 AND outgoing = 0 ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("store: files: %w", err)
	}
	defer rows.Close()
	var out []FileRec
	for rows.Next() {
		r, ok, err := scanFile(rows, d.box)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, r)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: files: %w", err)
	}
	return out, nil
}

func scanFile(s scanner, bx box) (FileRec, bool, error) {
	var (
		r              FileRec
		fid, pb        []byte
		outgoing, done int
		manCt, pathCt  []byte
	)
	err := s.Scan(&fid, &pb, &outgoing, &manCt, &pathCt, &r.Bitmap, &done, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return FileRec{}, false, nil
	}
	if err != nil {
		return FileRec{}, false, fmt.Errorf("store: scan file: %w", err)
	}
	copy(r.FileID[:], fid)
	copy(r.Peer[:], pb)
	r.Outgoing = outgoing != 0
	r.Done = done != 0
	man, err := bx.open(manCt, aadFileManifest(r.FileID))
	if err != nil {
		return FileRec{}, false, err
	}
	r.Manifest = man
	path, err := bx.open(pathCt, aadFilePath(r.FileID))
	if err != nil {
		return FileRec{}, false, err
	}
	r.Path = string(path)
	return r, true, nil
}

func aadFileManifest(fileID [16]byte) []byte {
	out := make([]byte, 0, len("files.manifest")+16)
	out = append(out, "files.manifest"...)
	out = append(out, fileID[:]...)
	return out
}

func aadFilePath(fileID [16]byte) []byte {
	out := make([]byte, 0, len("files.path")+16)
	out = append(out, "files.path"...)
	out = append(out, fileID[:]...)
	return out
}
