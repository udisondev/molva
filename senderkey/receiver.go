package senderkey

import (
	"crypto/ed25519"
	"encoding/binary"
)

// skippedKey — отложенный ключ пропущенного сообщения.
type skippedKey struct {
	n  uint32
	mk [32]byte
}

// Receiver — чужой sender key: приёмная цепочка, ключ проверки подписи и
// отложенные ключи внеочередных сообщений.
type Receiver struct {
	generation uint32
	ck         [32]byte
	n          uint32
	signPub    [32]byte
	skipped    []skippedKey
}

// NewReceiver строит приёмник из раздачи владельца.
func NewReceiver(d Dist) *Receiver {
	return &Receiver{generation: d.Generation, ck: d.ChainKey, n: d.N, signPub: d.SignPub}
}

// Generation — поколение ключа.
func (r *Receiver) Generation() uint32 { return r.generation }

// Dist — текущая точка приёмной цепочки: ею можно поделиться с новичком
// (он прочтёт всё с этого места и ничего до — отложенные ключи не уходят).
func (r *Receiver) Dist() Dist {
	return Dist{Generation: r.generation, ChainKey: r.ck, N: r.n, SignPub: r.signPub}
}

// Decrypt проверяет подпись и расшифровывает сообщение n, продвигая
// цепочку (пропущенные откладываются, старые гибнут).
func (r *Receiver) Decrypt(groupID [32]byte, generation, n uint32, ciphertext, signature []byte) ([]byte, error) {
	if generation > r.generation {
		return nil, ErrFutureKey
	}
	if generation < r.generation {
		return nil, ErrOldMessage
	}
	if !ed25519.Verify(r.signPub[:], signTranscript(groupID, generation, n, ciphertext), signature) {
		return nil, ErrBadSignature
	}
	if n < r.n {
		if mk, ok := r.takeSkipped(n); ok {
			return open(mk, groupID, n, ciphertext)
		}
		return nil, ErrOldMessage
	}
	if n-r.n > MaxSkip {
		return nil, ErrTooManySkipped
	}
	for r.n < n {
		mk, next := kdfChain(r.ck)
		r.skipped = append(r.skipped, skippedKey{n: r.n, mk: mk})
		if len(r.skipped) > MaxSkip {
			r.skipped = r.skipped[1:]
		}
		r.ck = next
		r.n++
	}
	mk, next := kdfChain(r.ck)
	plain, err := open(mk, groupID, n, ciphertext)
	if err != nil {
		return nil, err
	}
	r.ck = next
	r.n++
	return plain, nil
}

func (r *Receiver) takeSkipped(n uint32) ([32]byte, bool) {
	for i, e := range r.skipped {
		if e.n == n {
			r.skipped = append(r.skipped[:i], r.skipped[i+1:]...)
			return e.mk, true
		}
	}
	return [32]byte{}, false
}

// Marshal — сериализация для шифрованного хранения.
func (r *Receiver) Marshal() []byte {
	out := make([]byte, 0, 8+64+4+len(r.skipped)*36)
	out = binary.BigEndian.AppendUint32(out, r.generation)
	out = binary.BigEndian.AppendUint32(out, r.n)
	out = append(out, r.ck[:]...)
	out = append(out, r.signPub[:]...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(r.skipped)))
	for _, e := range r.skipped {
		out = binary.BigEndian.AppendUint32(out, e.n)
		out = append(out, e.mk[:]...)
	}
	return out
}

// UnmarshalReceiver восстанавливает приёмник.
func UnmarshalReceiver(b []byte) (*Receiver, error) {
	if len(b) < 76 {
		return nil, ErrMalformed
	}
	r := &Receiver{
		generation: binary.BigEndian.Uint32(b),
		n:          binary.BigEndian.Uint32(b[4:]),
	}
	copy(r.ck[:], b[8:40])
	copy(r.signPub[:], b[40:72])
	cnt := binary.BigEndian.Uint32(b[72:])
	if cnt > MaxSkip || len(b) != 76+int(cnt)*36 {
		return nil, ErrMalformed
	}
	off := 76
	for range cnt {
		var e skippedKey
		e.n = binary.BigEndian.Uint32(b[off:])
		copy(e.mk[:], b[off+4:off+36])
		r.skipped = append(r.skipped, e)
		off += 36
	}
	return r, nil
}
