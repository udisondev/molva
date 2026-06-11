package envelope

import (
	"bytes"
	"testing"
)

// FuzzDecode: произвольный вход не паникует; успешный разбор обязан
// пережить round-trip без расхождений.
func FuzzDecode(f *testing.F) {
	f.Add([]byte{})
	valid, _ := Encode(Envelope{MsgID: MsgID{1, 2, 3}, Type: TypeChat, FromSeq: 7, Payload: []byte("x")})
	f.Add(valid)
	f.Add([]byte{0x0a, 0x04, 1, 2, 3, 4, 0x10, 0x01}) // короткий msg_id
	f.Add([]byte{0xff, 0xff, 0xff})                   // мусор

	f.Fuzz(func(t *testing.T, data []byte) {
		e, err := Decode(data)
		if err != nil {
			return // отвергать кривой вход — норма; суть в отсутствии паник
		}
		b, err := Encode(e)
		if err != nil {
			t.Fatalf("декодированное не кодируется обратно: %v", err)
		}
		e2, err := Decode(b)
		if err != nil {
			t.Fatalf("re-decode: %v", err)
		}
		if e2.MsgID != e.MsgID || e2.Type != e.Type || e2.FromSeq != e.FromSeq ||
			e2.LamportTS != e.LamportTS || !bytes.Equal(e2.Payload, e.Payload) {
			t.Fatal("round-trip разошёлся")
		}
	})
}
