package blob

import (
	"bytes"
	"testing"
)

// FuzzDecodeManifest: манифест приходит от пира — произвольные байты не
// паникуют, успех переживает round-trip.
func FuzzDecodeManifest(f *testing.F) {
	f.Add([]byte{})
	valid, _ := EncodeManifest(Manifest{
		FileID: [16]byte{1}, Name: "файл.bin", Mime: "application/octet-stream",
		Size: 100, ChunkSize: ChunkSize, FileKey: [32]byte{2}, WholeHash: [32]byte{3},
	})
	f.Add(valid)
	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := DecodeManifest(data)
		if err != nil {
			return
		}
		b, err := EncodeManifest(m)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		m2, err := DecodeManifest(b)
		if err != nil || m2 != m {
			t.Fatalf("round-trip разошёлся: %v", err)
		}
	})
}

// FuzzDecodeRequest: запросы окон — недоверенный ввод.
func FuzzDecodeRequest(f *testing.F) {
	f.Add([]byte{})
	valid, _ := EncodeRequest(Request{FileID: [16]byte{1}, Indexes: []uint32{0, 1, 5}})
	f.Add(valid)
	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := DecodeRequest(data)
		if err != nil {
			return
		}
		if len(r.Indexes) == 0 || len(r.Indexes) > Window {
			t.Fatal("валидация окна пропустила мусор")
		}
	})
}

// FuzzDecodeChunk: чанки — недоверенный ввод.
func FuzzDecodeChunk(f *testing.F) {
	f.Add([]byte{})
	valid, _ := EncodeChunk(Chunk{FileID: [16]byte{1}, Index: 3, Payload: []byte("x")})
	f.Add(valid)
	f.Fuzz(func(t *testing.T, data []byte) {
		c, err := DecodeChunk(data)
		if err != nil {
			return
		}
		b, err := EncodeChunk(c)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		c2, err := DecodeChunk(b)
		if err != nil || c2.FileID != c.FileID || c2.Index != c.Index || !bytes.Equal(c2.Payload, c.Payload) {
			t.Fatalf("round-trip разошёлся: %v", err)
		}
	})
}

func TestBitmap(t *testing.T) {
	bm := NewBitmap(10)
	if bm.Complete() || bm.Count() != 0 {
		t.Fatal("пустой битмап")
	}
	if !bm.Set(3) || bm.Set(3) {
		t.Fatal("Set должен быть однократным")
	}
	if !bm.Has(3) || bm.Has(4) {
		t.Fatal("Has")
	}
	missing := bm.Missing(4)
	if len(missing) != 4 || missing[0] != 0 || missing[3] != 4 {
		t.Fatalf("Missing: %v", missing)
	}
	for i := range 10 {
		bm.Set(i)
	}
	if !bm.Complete() {
		t.Fatal("Complete")
	}
	// Round-trip через сериализацию.
	bm2 := BitmapFromBytes(bm.Bytes(), 10)
	if !bm2.Complete() {
		t.Fatal("сериализация потеряла биты")
	}
}
