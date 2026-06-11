package ratchet

import (
	"bytes"
	"testing"
)

// FuzzDecodeMessage: произвольный вход не паникует; успех — round-trip.
func FuzzDecodeMessage(f *testing.F) {
	f.Add([]byte{})
	valid, _ := EncodeMessage(Message{DHPub: [32]byte{1}, PN: 2, N: 3, Ciphertext: []byte("ct")})
	f.Add(valid)
	f.Add([]byte{0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := DecodeMessage(data)
		if err != nil {
			return
		}
		b, err := EncodeMessage(m)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		m2, err := DecodeMessage(b)
		if err != nil {
			t.Fatalf("re-decode: %v", err)
		}
		if m2.DHPub != m.DHPub || m2.PN != m.PN || m2.N != m.N || !bytes.Equal(m2.Ciphertext, m.Ciphertext) {
			t.Fatal("round-trip разошёлся")
		}
	})
}

// FuzzDecodeInit: рукопожатие из недоверенного входа.
func FuzzDecodeInit(f *testing.F) {
	f.Add([]byte{})
	valid, _ := EncodeInit(Init{IK: [32]byte{1}, Eph: [32]byte{2}, SID: [16]byte{3}})
	f.Add(valid)

	f.Fuzz(func(t *testing.T, data []byte) {
		i, err := DecodeInit(data)
		if err != nil {
			return
		}
		b, err := EncodeInit(i)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		i2, err := DecodeInit(b)
		if err != nil || i2 != i {
			t.Fatalf("round-trip разошёлся: %v", err)
		}
	})
}

// FuzzDecodeInitAck: ответ рукопожатия из недоверенного входа.
func FuzzDecodeInitAck(f *testing.F) {
	f.Add([]byte{})
	valid, _ := EncodeInitAck(InitAck{IK: [32]byte{1}, Eph: [32]byte{2}, SID: [16]byte{3}})
	f.Add(valid)

	f.Fuzz(func(t *testing.T, data []byte) {
		a, err := DecodeInitAck(data)
		if err != nil {
			return
		}
		b, err := EncodeInitAck(a)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		a2, err := DecodeInitAck(b)
		if err != nil || a2 != a {
			t.Fatalf("round-trip разошёлся: %v", err)
		}
	})
}

// FuzzUnmarshalState: состояние читается только своё, но рестарт после
// порчи диска не должен ронять процесс.
func FuzzUnmarshalState(f *testing.F) {
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		s, err := Unmarshal(data)
		if err != nil {
			return
		}
		if _, err := s.Marshal(); err != nil {
			t.Fatalf("re-marshal: %v", err)
		}
	})
}
