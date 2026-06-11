// Package senderkey — групповая криптография molva: у каждого участника
// свой sender key на группу (симметричная цепочка только вперёд) и
// Ed25519-подпись сообщений. Сообщение шифруется один раз, рассылается
// веером; новичок получает текущее состояние цепочки и не читает историю.
// Чистая логика без I/O.
package senderkey

import (
	"crypto/hkdf"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/chacha20poly1305"
)

const (
	// MaxSkip — потолок отложенных ключей пропущенных сообщений на цепочку.
	MaxSkip = 512

	labelMsgKey    = "molva/sk/mk/v1"
	labelChainKey  = "molva/sk/ck/v1"
	labelSignature = "molva/sk/sig/v1"
)

// Ошибки цепочки и подписи.
var (
	ErrDecrypt        = errors.New("senderkey: расшифровка не прошла")
	ErrBadSignature   = errors.New("senderkey: подпись не сходится")
	ErrTooManySkipped = errors.New("senderkey: слишком большая дыра в цепочке")
	ErrOldMessage     = errors.New("senderkey: сообщение старее цепочки")
	// ErrFutureKey — сообщение нового поколения: ключ rekey ещё едет,
	// доставку стоит переиграть позже.
	ErrFutureKey = errors.New("senderkey: ключ нового поколения ещё не получен")
	ErrMalformed = errors.New("senderkey: не разбирается")
)

// Dist — раздаваемое состояние ключа: текущая точка цепочки.
type Dist struct {
	Generation uint32
	ChainKey   [32]byte
	N          uint32
	SignPub    [32]byte
}

func kdfChain(ck [32]byte) (mk, next [32]byte) {
	mb, err := hkdf.Key(sha256.New, ck[:], nil, labelMsgKey, 32)
	if err != nil {
		panic("senderkey: mk: " + err.Error())
	}
	nb, err := hkdf.Key(sha256.New, ck[:], nil, labelChainKey, 32)
	if err != nil {
		panic("senderkey: ck: " + err.Error())
	}
	copy(mk[:], mb)
	copy(next[:], nb)
	return mk, next
}

// signTranscript — каноничные байты под подпись сообщения.
func signTranscript(groupID [32]byte, generation, n uint32, ciphertext []byte) []byte {
	h, err := blake2b.New256(nil)
	if err != nil {
		panic("senderkey: transcript: " + err.Error())
	}
	h.Write([]byte(labelSignature))
	h.Write(groupID[:])
	h.Write([]byte{
		byte(generation >> 24), byte(generation >> 16), byte(generation >> 8), byte(generation),
		byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n),
	})
	h.Write(ciphertext)
	return h.Sum(nil)
}

func seal(mk [32]byte, groupID [32]byte, n uint32, plaintext []byte) []byte {
	aead, err := chacha20poly1305.New(mk[:])
	if err != nil {
		panic("senderkey: aead: " + err.Error())
	}
	nonce := make([]byte, chacha20poly1305.NonceSize)
	aad := append(groupID[:], byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	return aead.Seal(nil, nonce, plaintext, aad)
}

func open(mk [32]byte, groupID [32]byte, n uint32, ciphertext []byte) ([]byte, error) {
	aead, err := chacha20poly1305.New(mk[:])
	if err != nil {
		panic("senderkey: aead: " + err.Error())
	}
	nonce := make([]byte, chacha20poly1305.NonceSize)
	aad := append(groupID[:], byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	plain, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, ErrDecrypt
	}
	return plain, nil
}

// readKey читает 32 байта из r.
func readKey(r io.Reader) ([32]byte, error) {
	var k [32]byte
	if _, err := io.ReadFull(r, k[:]); err != nil {
		return k, fmt.Errorf("senderkey: rand: %w", err)
	}
	return k, nil
}
