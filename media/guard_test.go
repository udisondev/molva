package media

import (
	"errors"
	"testing"

	"github.com/udisondev/nodenet/transport"
)

// Кадр с зарезервированным каналом или oversize-датаграммой отвергается
// без паники: иначе nodenet уронил бы ядро на недоверенном кадре из IPC.
func TestSendGuardsAgainstPanic(t *testing.T) {
	// Зарезервированный канал отсекается ещё до сессии.
	b := NewBridge(nil, nil)
	if err := b.Send(0, []byte("x")); !errors.Is(err, ErrBadFrame) {
		t.Fatalf("канал 0: want ErrBadFrame, got %v", err)
	}
	if err := b.Send(transport.FirstAppChannel-1, []byte("x")); !errors.Is(err, ErrBadFrame) {
		t.Fatalf("зарезервированный канал: want ErrBadFrame, got %v", err)
	}

	out, in := pair(t)
	defer out.Close()
	defer in.Close()
	b.Attach(out)

	// Датаграмма больше потолка медиаканала — nodenet паникует, мы отсекаем.
	big := make([]byte, transport.MaxMediaDatagram+1)
	if err := b.Send(ChAudio, big); !errors.Is(err, ErrBadFrame) {
		t.Fatalf("oversize-датаграмма: want ErrBadFrame, got %v", err)
	}
	// Нормальный кадр проходит.
	if err := b.Send(ChAudio, []byte("opus")); err != nil {
		t.Fatalf("нормальный кадр: %v", err)
	}
	if b.Stats().TxBadFrame != 3 {
		t.Fatalf("счётчик плохих кадров: %d, want 3", b.Stats().TxBadFrame)
	}
}
