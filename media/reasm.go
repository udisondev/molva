package media

import (
	"encoding/binary"
	"errors"
)

// Видеокадр едет надёжными one-shot-сообщениями (без HOL между кадрами),
// но кадр больше потолка сообщения дробится на сегменты с маленьким
// заголовком: [frame_id u32][seg u8][total u8][payload]. Порядок между
// сообщениями не гарантируется — реассемблер собирает по frame_id.

const (
	segHeaderLen = 6
	// segPayload — полезная нагрузка сегмента: запас от кадра nodenet.
	segPayload = 60 << 10
	// MaxVideoFrame — потолок одного видеокадра (ключевой 720p VP8 с запасом).
	MaxVideoFrame = 1 << 20
	// reasmSlots — сколько кадров собирается одновременно (сообщения могут
	// перемешиваться); старейший вытесняется.
	reasmSlots = 4
)

// ErrBadSegment — сегмент не разбирается или противоречит собрату.
var ErrBadSegment = errors.New("media: кривой сегмент видеокадра")

// segmentVideo дробит кадр; out переиспользуется между вызовами.
func segmentVideo(frameID uint32, frame []byte, emit func(seg []byte) error) error {
	if len(frame) == 0 || len(frame) > MaxVideoFrame {
		return ErrBadSegment
	}
	total := (len(frame) + segPayload - 1) / segPayload
	if total > 255 {
		return ErrBadSegment
	}
	var buf [segHeaderLen + segPayload]byte
	for i := range total {
		start := i * segPayload
		end := min(start+segPayload, len(frame))
		binary.BigEndian.PutUint32(buf[:4], frameID)
		buf[4] = byte(i)
		buf[5] = byte(total)
		n := copy(buf[segHeaderLen:], frame[start:end])
		if err := emit(buf[:segHeaderLen+n]); err != nil {
			return err
		}
	}
	return nil
}

// reasmFrame — один собираемый кадр.
type reasmFrame struct {
	id    uint32
	total int
	have  int
	parts [][]byte
}

// Reassembler собирает видеокадры из сегментов; не потокобезопасен
// (живёт на приёмной горутине моста).
type Reassembler struct {
	slots []*reasmFrame
}

// NewReassembler — пустой реассемблер.
func NewReassembler() *Reassembler { return &Reassembler{} }

// Push принимает сегмент (копирует данные); вернувшийся кадр полон.
func (r *Reassembler) Push(seg []byte) ([]byte, error) {
	if len(seg) <= segHeaderLen {
		return nil, ErrBadSegment
	}
	id := binary.BigEndian.Uint32(seg[:4])
	idx, total := int(seg[4]), int(seg[5])
	if total == 0 || idx >= total || len(seg)-segHeaderLen > segPayload {
		return nil, ErrBadSegment
	}

	var f *reasmFrame
	for _, s := range r.slots {
		if s.id == id {
			f = s
			break
		}
	}
	if f == nil {
		f = &reasmFrame{id: id, total: total, parts: make([][]byte, total)}
		r.slots = append(r.slots, f)
		if len(r.slots) > reasmSlots {
			r.slots = r.slots[1:] // старейший кадр гибнет (поздно собирать)
		}
	}
	if f.total != total {
		return nil, ErrBadSegment
	}
	if f.parts[idx] != nil {
		return nil, nil // дубль сегмента
	}
	f.parts[idx] = append([]byte(nil), seg[segHeaderLen:]...)
	f.have++
	if f.have < f.total {
		return nil, nil
	}

	// Кадр полон: склеить и освободить слот.
	for i, s := range r.slots {
		if s == f {
			r.slots = append(r.slots[:i], r.slots[i+1:]...)
			break
		}
	}
	size := 0
	for _, p := range f.parts {
		size += len(p)
	}
	if size > MaxVideoFrame {
		return nil, ErrBadSegment
	}
	out := make([]byte, 0, size)
	for _, p := range f.parts {
		out = append(out, p...)
	}
	return out, nil
}
