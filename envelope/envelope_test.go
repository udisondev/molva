package envelope

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	mid, err := NewMsgID(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	in := Envelope{
		MsgID:     mid,
		Type:      TypeChat,
		FromSeq:   42,
		LamportTS: 100,
		Payload:   []byte("шифртекст"),
	}
	b, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if out.MsgID != in.MsgID || out.Type != in.Type || out.FromSeq != in.FromSeq ||
		out.LamportTS != in.LamportTS || !bytes.Equal(out.Payload, in.Payload) {
		t.Fatalf("round-trip разошёлся: %+v != %+v", out, in)
	}
}

func TestEncodeRejects(t *testing.T) {
	mid := MsgID{1}
	tests := []struct {
		name string
		env  Envelope
		want error
	}{
		{"нулевой тип", Envelope{MsgID: mid, Type: 0}, ErrBadType},
		{"неизвестный тип", Envelope{MsgID: mid, Type: maxType + 1}, ErrBadType},
		{"огромный payload", Envelope{MsgID: mid, Type: TypeChat, Payload: make([]byte, MaxPayload+1)}, ErrTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Encode(tc.env); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestDecodeRejects(t *testing.T) {
	// Корректный protobuf, но msg_id короткий.
	short, err := Encode(Envelope{MsgID: MsgID{1}, Type: TypeAck})
	if err != nil {
		t.Fatal(err)
	}
	// Подменяем длину msg_id нельзя без ручной сборки — соберём вручную:
	// поле 1 (bytes, 4 байта), поле 2 (varint 1).
	raw := []byte{0x0a, 0x04, 1, 2, 3, 4, 0x10, 0x01}
	if _, err := Decode(raw); !errors.Is(err, ErrBadMsgID) {
		t.Fatalf("короткий msg_id: err = %v, want %v", err, ErrBadMsgID)
	}
	_ = short

	if _, err := Decode([]byte{0xff, 0xff, 0xff}); !errors.Is(err, ErrMalformed) {
		t.Fatalf("мусор: err = %v, want %v", err, ErrMalformed)
	}
}
