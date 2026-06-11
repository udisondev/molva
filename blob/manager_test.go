package blob

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/store"
	"golang.org/x/crypto/blake2b"
)

var (
	senderID   = peer.ID{0x5E}
	receiverID = peer.ID{0x4C}
)

// Обрыв на ~70% и резюм: транспорт дропает чанки после порога, окно
// переигрывается по таймауту впустую, после «починки» докачка завершается,
// файл сходится с манифестом побайтово.
func TestPullResumeAfterLoss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dirS, dirR := t.TempDir(), t.TempDir()
		dbS := openDB(t, dirS)
		dbR := openDB(t, dirR)

		// Файл: 300 чанков по 1 КиБ + рваный хвост.
		const chunkSize = 1024
		content := make([]byte, 300*chunkSize+123)
		if _, err := rand.Read(content); err != nil {
			t.Fatal(err)
		}
		srcPath := filepath.Join(dirS, "исходник.bin")
		if err := os.WriteFile(srcPath, content, 0o600); err != nil {
			t.Fatal(err)
		}
		man := Manifest{
			Name: "исходник.bin", Mime: "application/octet-stream",
			Size: uint64(len(content)), ChunkSize: chunkSize,
		}
		if _, err := rand.Read(man.FileID[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := rand.Read(man.FileKey[:]); err != nil {
			t.Fatal(err)
		}
		man.WholeHash = blake2b.Sum256(content)
		manBytes, err := EncodeManifest(man)
		if err != nil {
			t.Fatal(err)
		}

		// Дроп после порога ~70% (210 из 301 чанка).
		var delivered atomic.Int64
		var dropping atomic.Bool
		dropping.Store(true)
		var dropped atomic.Int64

		var recv *Manager
		send := &Manager{
			db: dbS, dir: dirS, rnd: rand.Reader,
			sendReq: func(context.Context, peer.ID, []byte) error { return nil },
			online:  func(peer.ID) bool { return true },
			sendChunk: func(_ context.Context, _ peer.ID, frame []byte) error {
				if dropping.Load() && delivered.Load() >= 210 {
					dropped.Add(1)
					return nil // молча в дыру — как сеть
				}
				delivered.Add(1)
				env, err := envelope.Decode(frame)
				if err != nil {
					t.Errorf("кадр чанка: %v", err)
					return nil
				}
				recv.HandleChunk(senderID, &env)
				return nil
			},
			inChunks: make(chan inFrame, 256),
			inReqs:   make(chan inFrame, 64),
			newPulls: make(chan store.FileRec, 4),
			pulls:    map[[16]byte]*pull{},
		}
		done := make(chan string, 1)
		recv = &Manager{
			db: dbR, dir: dirR, rnd: rand.Reader,
			sendChunk: func(context.Context, peer.ID, []byte) error { return nil },
			online:    func(peer.ID) bool { return true },
			sendReq: func(_ context.Context, _ peer.ID, frame []byte) error {
				env, err := envelope.Decode(frame)
				if err != nil {
					t.Errorf("кадр запроса: %v", err)
					return nil
				}
				send.HandleChunkReq(receiverID, &env)
				return nil
			},
			inChunks: make(chan inFrame, 256),
			inReqs:   make(chan inFrame, 64),
			newPulls: make(chan store.FileRec, 4),
			pulls:    map[[16]byte]*pull{},
			onDone:   func(_ [16]byte, path string) { done <- path },
		}

		now := time.Now().UnixMilli()
		err = dbS.Tx(ctx, func(tx *store.Tx) error {
			return tx.FilePut(&store.FileRec{
				FileID: man.FileID, Peer: receiverID, Outgoing: true,
				Manifest: manBytes, Path: srcPath, Done: true,
				CreatedAt: now, UpdatedAt: now,
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		err = dbR.Tx(ctx, func(tx *store.Tx) error {
			return tx.FilePut(&store.FileRec{
				FileID: man.FileID, Peer: senderID, Outgoing: false,
				Manifest: manBytes,
				Path:     filepath.Join(dirR, fmt.Sprintf("%x.part", man.FileID)),
				Bitmap:   NewBitmap(man.Chunks()).Bytes(),
				CreatedAt: now, UpdatedAt: now,
			})
		})
		if err != nil {
			t.Fatal(err)
		}

		go func() { _ = send.Run(ctx) }()
		go func() { _ = recv.Run(ctx) }() // резюм подхватит незавершённый приём

		// Дать оборваться: приём застревает на пороге, окна ретраятся впустую.
		time.Sleep(30 * time.Second)
		synctest.Wait()
		if got := recv.Stats().ChunksRecv; got != 210 {
			t.Fatalf("до починки принято %d, want 210", got)
		}
		if dropped.Load() == 0 {
			t.Fatal("дроп не сработал — тест не про то")
		}

		// Битмап персистится по ходу — резюм не с нуля.
		rec, ok, err := dbR.FileGet(ctx, man.FileID)
		if err != nil || !ok {
			t.Fatalf("запись приёма пропала: %v", err)
		}
		if BitmapFromBytes(rec.Bitmap, man.Chunks()).Count() == 0 {
			t.Fatal("битмап не сохранялся по ходу приёма")
		}

		// Починка: следующий ретрай окна дотягивает хвост.
		dropping.Store(false)
		var final string
		deadline := time.Now().Add(5 * time.Minute)
		for {
			synctest.Wait()
			select {
			case final = <-done:
			default:
			}
			if final != "" || time.Now().After(deadline) {
				break
			}
			time.Sleep(time.Second)
		}
		if final == "" {
			t.Fatalf("докачка не завершилась: %+v", recv.Stats())
		}

		got, err := os.ReadFile(final)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, content) {
			t.Fatal("файл разошёлся с исходником")
		}
		st := recv.Stats()
		if st.BadChunks != 0 || st.HashMismatch != 0 {
			t.Fatalf("счётчики порчи не пусты: %+v", st)
		}
		rec, _, _ = dbR.FileGet(ctx, man.FileID)
		if !rec.Done || rec.Path != final {
			t.Fatalf("запись не финализирована: %+v", rec)
		}

		// Подделка: битый payload гибнет на AEAD, файл не трогается.
		evil := Chunk{FileID: man.FileID, Index: 0, Payload: []byte("мусор-мусор-мусор")}
		evilBytes, _ := EncodeChunk(evil)
		env := envelope.Envelope{MsgID: envelope.MsgID{9}, Type: envelope.TypeFileChunk, Payload: evilBytes}
		recv.HandleChunk(senderID, &env)
		synctest.Wait()
		if recv.Stats().LateChunks == 0 && recv.Stats().BadChunks == 0 {
			t.Fatal("подделка не учтена")
		}
	})
}

func openDB(t *testing.T, dir string) *store.DB {
	t.Helper()
	d, err := store.Open(filepath.Join(dir, "molva.db"), store.KeyFromSeed([32]byte{0xF1}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}
