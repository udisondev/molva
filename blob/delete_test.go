package blob

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/store"
)

// Манифест с раздутым числом чанков (малый ChunkSize при большом Size)
// отвергается до аллокации битмапа.
func TestManifestChunkCap(t *testing.T) {
	huge := Manifest{
		Name: "evil.bin", Mime: "application/octet-stream",
		Size: MaxFileSize, ChunkSize: 1, // миллиарды чанков
	}
	if _, err := EncodeManifest(huge); err == nil {
		t.Fatal("манифест с числом чанков выше потолка должен отвергаться")
	}
	// Файл предельного размера при штатном чанке (~71k чанков) — проходит.
	ok := Manifest{
		Name: "ok.bin", Mime: "application/octet-stream",
		Size: MaxFileSize, ChunkSize: ChunkSize,
	}
	if _, err := rand.Read(ok.FileID[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(ok.FileKey[:]); err != nil {
		t.Fatal(err)
	}
	if ok.Chunks() > maxChunks {
		t.Fatalf("реальный максимум чанков %d превысил потолок %d", ok.Chunks(), maxChunks)
	}
	if _, err := EncodeManifest(ok); err != nil {
		t.Fatalf("манифест предельного файла должен проходить: %v", err)
	}
}

// DeleteFile стирает запись передачи и сам blob с диска.
func TestDeleteFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db := openDB(t, dir)

	var fileID [16]byte
	if _, err := rand.Read(fileID[:]); err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(dir, fmt.Sprintf("%x-получено.bin", fileID[:4]))
	if err := os.WriteFile(final, []byte("содержимое"), 0o600); err != nil {
		t.Fatal(err)
	}
	man := Manifest{Name: "получено.bin", Mime: "application/octet-stream", Size: 10, ChunkSize: ChunkSize, FileID: fileID}
	manBytes, err := EncodeManifest(man)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	if err := db.Tx(ctx, func(tx *store.Tx) error {
		return tx.FilePut(&store.FileRec{
			FileID: fileID, Peer: peer.ID{0x4C}, Outgoing: false,
			Manifest: manBytes, Path: final, Done: true, CreatedAt: now, UpdatedAt: now,
		})
	}); err != nil {
		t.Fatal(err)
	}

	m := &Manager{db: db, dir: dir, dropPulls: make(chan [16]byte, 1)}
	if err := m.DeleteFile(ctx, fileID); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatal("blob файла остался на диске после удаления")
	}
	if _, ok, _ := db.FileGet(ctx, fileID); ok {
		t.Fatal("запись передачи осталась после удаления")
	}
	if m.Stats().FilesDeleted != 1 {
		t.Fatalf("счётчик удалений: %d, want 1", m.Stats().FilesDeleted)
	}
}
