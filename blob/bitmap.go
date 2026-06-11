package blob

// Bitmap — отметки принятых чанков; сериализуется как есть в store и
// после рестарта продолжает докачку с того же места.
type Bitmap struct {
	bits []byte
	n    int
}

// NewBitmap — пустой битмап на n чанков.
func NewBitmap(n int) *Bitmap {
	return &Bitmap{bits: make([]byte, (n+7)/8), n: n}
}

// BitmapFromBytes восстанавливает битмап из сериализации; кривая длина —
// свежий пустой (перекачаем лишнее, но не сломаемся).
func BitmapFromBytes(b []byte, n int) *Bitmap {
	if len(b) != (n+7)/8 {
		return NewBitmap(n)
	}
	bits := make([]byte, len(b))
	copy(bits, b)
	return &Bitmap{bits: bits, n: n}
}

// Bytes — сериализация для store.
func (m *Bitmap) Bytes() []byte {
	out := make([]byte, len(m.bits))
	copy(out, m.bits)
	return out
}

// Set отмечает чанк принятым; false — был отмечен раньше.
func (m *Bitmap) Set(i int) bool {
	if i < 0 || i >= m.n {
		return false
	}
	mask := byte(1) << (i % 8)
	if m.bits[i/8]&mask != 0 {
		return false
	}
	m.bits[i/8] |= mask
	return true
}

// Has — принят ли чанк.
func (m *Bitmap) Has(i int) bool {
	if i < 0 || i >= m.n {
		return false
	}
	return m.bits[i/8]&(byte(1)<<(i%8)) != 0
}

// Count — сколько чанков принято.
func (m *Bitmap) Count() int {
	c := 0
	for i := range m.n {
		if m.Has(i) {
			c++
		}
	}
	return c
}

// Complete — все ли чанки на месте.
func (m *Bitmap) Complete() bool { return m.Count() == m.n }

// Missing — до limit первых недостающих индексов.
func (m *Bitmap) Missing(limit int) []uint32 {
	var out []uint32
	for i := 0; i < m.n && len(out) < limit; i++ {
		if !m.Has(i) {
			out = append(out, uint32(i))
		}
	}
	return out
}
