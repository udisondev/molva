package media

import "testing"

// FuzzReassemblerPush — реассемблер видеокадров принимает недоверенные
// сегменты из медиаканала: произвольный ввод не должен паниковать, а
// собранный кадр — не превышать потолок.
func FuzzReassemblerPush(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 1, 0, 1, 0xAA})
	f.Add([]byte{0, 0, 0, 1, 0, 0, 0xAA})       // total=0
	f.Add([]byte{0, 0, 0, 1, 5, 2, 0xAA})       // idx>=total
	f.Add(make([]byte, segHeaderLen+segPayload)) // полный сегмент
	f.Fuzz(func(t *testing.T, seg []byte) {
		r := NewReassembler()
		frame, err := r.Push(seg)
		if err == nil && frame != nil && len(frame) > MaxVideoFrame {
			t.Fatalf("собранный кадр %d превышает потолок %d", len(frame), MaxVideoFrame)
		}
	})
}

// FuzzReassemblerRoundtrip — корректный кадр, нарезанный segmentVideo,
// собирается обратно байт-в-байт независимо от порядка сегментов.
func FuzzReassemblerRoundtrip(f *testing.F) {
	f.Add(uint32(1), []byte("кадр"))
	f.Add(uint32(7), make([]byte, segPayload+10))
	f.Fuzz(func(t *testing.T, id uint32, frame []byte) {
		if len(frame) == 0 || len(frame) > MaxVideoFrame {
			return
		}
		var segs [][]byte
		if err := segmentVideo(id, frame, func(seg []byte) error {
			segs = append(segs, append([]byte(nil), seg...))
			return nil
		}); err != nil {
			return
		}
		r := NewReassembler()
		var out []byte
		for _, seg := range segs {
			got, err := r.Push(seg)
			if err != nil {
				t.Fatalf("Push корректного сегмента: %v", err)
			}
			if got != nil {
				out = got
			}
		}
		if string(out) != string(frame) {
			t.Fatalf("кадр не восстановился: %d против %d байт", len(out), len(frame))
		}
	})
}

// FuzzDecodeFeedback — фидбек получателя (канал 18) — недоверенный ввод:
// разбор не паникует, валидный кадр восстанавливается.
func FuzzDecodeFeedback(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, feedbackLen))
	f.Fuzz(func(t *testing.T, b []byte) {
		period, received, ok := decodeFeedback(b)
		if !ok {
			return
		}
		round := encodeFeedback(period, received)
		if p2, r2, ok2 := decodeFeedback(round); !ok2 || p2 != period || r2 != received {
			t.Fatalf("round-trip фидбека разошёлся")
		}
	})
}
