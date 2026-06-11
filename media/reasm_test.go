package media

import (
	"bytes"
	"crypto/rand"
	"testing"
	"time"
)

func segmentsOf(t *testing.T, id uint32, frame []byte) [][]byte {
	t.Helper()
	var segs [][]byte
	err := segmentVideo(id, frame, func(seg []byte) error {
		segs = append(segs, bytes.Clone(seg))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return segs
}

func TestReassembleInOrderAndOutOfOrder(t *testing.T) {
	frame := make([]byte, 150_000) // 3 сегмента
	if _, err := rand.Read(frame); err != nil {
		t.Fatal(err)
	}
	segs := segmentsOf(t, 7, frame)
	if len(segs) != 3 {
		t.Fatalf("сегментов %d, want 3", len(segs))
	}

	// По порядку.
	r := NewReassembler()
	var got []byte
	for _, s := range segs {
		out, err := r.Push(s)
		if err != nil {
			t.Fatal(err)
		}
		if out != nil {
			got = out
		}
	}
	if !bytes.Equal(got, frame) {
		t.Fatal("кадр разошёлся (по порядку)")
	}

	// Вперемешку + дубль.
	r = NewReassembler()
	order := []int{2, 0, 0, 1}
	got = nil
	for _, i := range order {
		out, err := r.Push(segs[i])
		if err != nil {
			t.Fatal(err)
		}
		if out != nil {
			got = out
		}
	}
	if !bytes.Equal(got, frame) {
		t.Fatal("кадр разошёлся (вперемешку)")
	}
}

func TestReassemblerInterleavedFrames(t *testing.T) {
	f1 := bytes.Repeat([]byte{1}, 70_000)
	f2 := bytes.Repeat([]byte{2}, 70_000)
	s1 := segmentsOf(t, 1, f1)
	s2 := segmentsOf(t, 2, f2)

	r := NewReassembler()
	var done [][]byte
	for _, s := range [][]byte{s1[0], s2[0], s2[1], s1[1]} {
		out, err := r.Push(s)
		if err != nil {
			t.Fatal(err)
		}
		if out != nil {
			done = append(done, out)
		}
	}
	if len(done) != 2 || !bytes.Equal(done[0], f2) || !bytes.Equal(done[1], f1) {
		t.Fatalf("сборка перемешанных кадров сломана: %d", len(done))
	}
}

func TestReassemblerRejectsGarbage(t *testing.T) {
	r := NewReassembler()
	for _, b := range [][]byte{nil, {1, 2, 3}, bytes.Repeat([]byte{0}, segHeaderLen)} {
		if _, err := r.Push(b); err == nil {
			t.Fatalf("мусор принят: %v", b)
		}
	}
	// total=0 и idx>=total.
	bad := make([]byte, segHeaderLen+1)
	bad[5] = 0
	if _, err := r.Push(bad); err == nil {
		t.Fatal("total=0 принят")
	}
	bad[4], bad[5] = 5, 3
	if _, err := r.Push(bad); err == nil {
		t.Fatal("idx>=total принят")
	}
}

func TestAdapterLadder(t *testing.T) {
	var levels []Preset
	a := NewAdapter(Preset720, func(p Preset) { levels = append(levels, p) })

	// Локальные дропы — вниз.
	a.ObserveTxDrops(1)
	a.ObserveTxDrops(1) // не выросло — без эффекта
	a.ObserveTxDrops(3)
	if a.Level() != Preset240 {
		t.Fatalf("после двух деградаций: %d", a.Level())
	}
	// Растянутый фидбек — вниз до пола.
	a.ObserveFeedback((200 * time.Millisecond).Microseconds(), 5)
	a.ObserveFeedback((200 * time.Millisecond).Microseconds(), 5)
	if a.Level() != PresetAudioOnly {
		t.Fatalf("пол лестницы: %d", a.Level())
	}
	// Нормальный фидбек не валит.
	a.ObserveFeedback((100 * time.Millisecond).Microseconds(), 5)

	// Вверх — только после стабильности.
	a.Tick(time.Now())
	if a.Level() != PresetAudioOnly {
		t.Fatal("рано вверх")
	}
	a.Tick(time.Now().Add(upgradeAfter + time.Second))
	if a.Level() != Preset240 {
		t.Fatalf("шаг вверх не случился: %d", a.Level())
	}
	if len(levels) != 4 {
		t.Fatalf("колбэков %d, want 4", len(levels))
	}
}
