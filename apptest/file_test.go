package apptest

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/udisondev/molva/chat"
)

// Полная передача файла через настоящий кластер: манифест по ratchet-сессии,
// чанки прямым ребром, верификация и побайтовое совпадение.
func TestFileTransferEndToEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 2)
		a, b := c.Node(0), c.Node(1)
		ctx := context.Background()
		MakeContacts(t, c, 0, 1)

		// Сессия появляется с первой перепиской.
		mid := sendText(t, a, b.PeerID(), "лови файл")
		WaitInboundMessage(t, b, a.PeerID(), mid, 5*time.Minute)

		content := make([]byte, 700*1024+17) // ~12 чанков, рваный хвост
		if _, err := rand.Read(content); err != nil {
			t.Fatal(err)
		}
		src := filepath.Join(t.TempDir(), "отчёт за квартал.pdf")
		if err := os.WriteFile(src, content, 0o600); err != nil {
			t.Fatal(err)
		}

		fileID, err := a.Core().Files().Offer(ctx, b.PeerID(), src)
		if err != nil {
			t.Fatalf("Offer: %v", err)
		}

		// Дождаться завершения приёма у B.
		deadline := time.Now().Add(10 * time.Minute)
		var final string
		for {
			synctest.Wait()
			rec, ok, err := b.Core().Store().FileGet(ctx, fileID)
			if err != nil {
				t.Fatal(err)
			}
			if ok && rec.Done {
				final = rec.Path
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("приём не завершился: %+v", b.Core().Files().Stats())
			}
			time.Sleep(2 * time.Second)
		}

		got, err := os.ReadFile(final)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, content) {
			t.Fatal("файл разошёлся с исходником")
		}
		if filepath.Base(final) == "" || filepath.Dir(final) != filepath.Join(b.DataDir(), "files") {
			t.Fatalf("файл лёг не туда: %s", final)
		}
		st := b.Core().Files().Stats()
		if st.BadChunks != 0 || st.HashMismatch != 0 {
			t.Fatalf("счётчики порчи не пусты: %+v", st)
		}
	})
}

// Оффер без установленной сессии: рукопожатие запускается, повтор после
// его завершения проходит.
func TestFileOfferStartsSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewCluster(t, 2)
		a, b := c.Node(0), c.Node(1)
		ctx := context.Background()
		MakeContacts(t, c, 0, 1)

		src := filepath.Join(t.TempDir(), "ранний.bin")
		if err := os.WriteFile(src, []byte("маленький файл"), 0o600); err != nil {
			t.Fatal(err)
		}

		// Сессии ещё нет: первый оффер честно отказывает и запускает её.
		_, err := a.Core().Files().Offer(ctx, b.PeerID(), src)
		if !errors.Is(err, chat.ErrNoSession) {
			t.Fatalf("err = %v, want ErrNoSession", err)
		}

		// Рукопожатие завершается само (обе стороны онлайн).
		deadline := time.Now().Add(5 * time.Minute)
		for {
			synctest.Wait()
			ready, err := a.Core().Chats().SessionReady(ctx, b.PeerID())
			if err != nil {
				t.Fatal(err)
			}
			if ready {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("сессия не установилась")
			}
			time.Sleep(2 * time.Second)
		}

		fileID, err := a.Core().Files().Offer(ctx, b.PeerID(), src)
		if err != nil {
			t.Fatalf("повторный Offer: %v", err)
		}
		deadline = time.Now().Add(5 * time.Minute)
		for {
			synctest.Wait()
			rec, ok, _ := b.Core().Store().FileGet(ctx, fileID)
			if ok && rec.Done {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("приём не завершился: %+v", b.Core().Files().Stats())
			}
			time.Sleep(2 * time.Second)
		}
	})
}
