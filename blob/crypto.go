package blob

import (
	"encoding/binary"

	"golang.org/x/crypto/chacha20poly1305"
)

// Чанки шифруются одноразовым файловым ключом из манифеста: AEAD с nonce
// из индекса даёт и конфиденциальность, и целостность, и привязку к
// позиции — отдельные хэши чанков не нужны. Файл целиком сверяется по
// BLAKE2b из манифеста после приёма.

func chunkNonce(index uint32) []byte {
	nonce := make([]byte, chacha20poly1305.NonceSize)
	binary.BigEndian.PutUint32(nonce[len(nonce)-4:], index)
	return nonce
}

// sealChunk шифрует содержимое чанка index файловым ключом.
func sealChunk(key [32]byte, fileID [16]byte, index uint32, plain []byte) []byte {
	aead, err := chacha20poly1305.New(key[:])
	if err != nil {
		panic("blob: aead: " + err.Error())
	}
	return aead.Seal(nil, chunkNonce(index), plain, fileID[:])
}

// openChunk расшифровывает и аутентифицирует чанк.
func openChunk(key [32]byte, fileID [16]byte, index uint32, payload []byte) ([]byte, error) {
	aead, err := chacha20poly1305.New(key[:])
	if err != nil {
		panic("blob: aead: " + err.Error())
	}
	plain, err := aead.Open(nil, chunkNonce(index), payload, fileID[:])
	if err != nil {
		return nil, ErrBadChunk
	}
	return plain, nil
}

// chunkLen — длина чанка index (последний короче).
func chunkLen(m *Manifest, index uint32) int {
	off := uint64(index) * uint64(m.ChunkSize)
	if off >= m.Size {
		return 0
	}
	rest := m.Size - off
	if rest > uint64(m.ChunkSize) {
		return int(m.ChunkSize)
	}
	return int(rest)
}
