// Package ratchet — канонический Double Ratchet личных диалогов molva:
// интерактивная инициализация по аутентифицированному каналу, DH-ratchet с
// двумя симметричными цепочками, отложенные ключи внеочередных сообщений.
// Чистая логика без I/O; персистентность состояния — забота вызывающего
// (store), сетевая доставка — конвертов и outbox.
package ratchet

import (
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/udisondev/molva/peer"
	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/chacha20poly1305"
)

// Метки доменов HKDF: ни один ключ не живёт в двух протоколах.
const (
	labelIdentity   = "molva/ratchet/identity/v1"
	labelSK         = "molva/dr/sk/v1"
	labelRoot       = "molva/dr/root/v1"
	labelMsgKey     = "molva/dr/mk/v1"
	labelChainKey   = "molva/dr/ck/v1"
	labelTranscript = "molva/dr/transcript/v1"
)

// IdentityFromSeed выводит identity-пару X25519 ratchet-слоя из master-seed.
// Ключи nodenet не переиспользуются: своя метка — свой ключ.
func IdentityFromSeed(seed [32]byte) *ecdh.PrivateKey {
	b, err := hkdf.Key(sha256.New, seed[:], nil, labelIdentity, 32)
	if err == nil {
		var key *ecdh.PrivateKey
		key, err = ecdh.X25519().NewPrivateKey(b)
		if err == nil {
			return key
		}
	}
	// Параметры статичны: ошибка возможна только при порче рантайма.
	panic("ratchet: identity из seed: " + err.Error())
}

// deriveSK сводит три DH-выхода интерактивного рукопожатия в стартовый
// root key, привязывая его к transcript'у сессии.
func deriveSK(dh1, dh2, dh3, transcript []byte) [32]byte {
	ikm := make([]byte, 0, len(dh1)+len(dh2)+len(dh3))
	ikm = append(ikm, dh1...)
	ikm = append(ikm, dh2...)
	ikm = append(ikm, dh3...)
	b, err := hkdf.Key(sha256.New, ikm, transcript, labelSK, 32)
	if err != nil {
		panic("ratchet: sk: " + err.Error())
	}
	var sk [32]byte
	copy(sk[:], b)
	return sk
}

// kdfRoot — шаг DH-ratchet'а: новый root key и стартовый chain key.
func kdfRoot(rk [32]byte, dhOut []byte) (newRK, ck [32]byte) {
	b, err := hkdf.Key(sha256.New, dhOut, rk[:], labelRoot, 64)
	if err != nil {
		panic("ratchet: root: " + err.Error())
	}
	copy(newRK[:], b[:32])
	copy(ck[:], b[32:])
	return newRK, ck
}

// kdfChain — шаг симметричной цепочки: одноразовый message key и
// следующий chain key (цепочка идёт только вперёд).
func kdfChain(ck [32]byte) (mk, next [32]byte) {
	mb, err := hkdf.Key(sha256.New, ck[:], nil, labelMsgKey, 32)
	if err != nil {
		panic("ratchet: mk: " + err.Error())
	}
	nb, err := hkdf.Key(sha256.New, ck[:], nil, labelChainKey, 32)
	if err != nil {
		panic("ratchet: ck: " + err.Error())
	}
	copy(mk[:], mb)
	copy(next[:], nb)
	return mk, next
}

// transcript — BLAKE2b-привязка сессии: роли, NodeID и все публичные ключи
// рукопожатия. Инициатор всегда первым — обе стороны считают одинаково.
func transcript(initiator, responder peer.ID, ikI, ikR, ephI, ephR []byte, sid [16]byte) []byte {
	h, err := blake2b.New256(nil)
	if err != nil {
		panic("ratchet: transcript: " + err.Error())
	}
	h.Write([]byte(labelTranscript))
	h.Write(initiator[:])
	h.Write(responder[:])
	h.Write(ikI)
	h.Write(ikR)
	h.Write(ephI)
	h.Write(ephR)
	h.Write(sid[:])
	return h.Sum(nil)
}

// seal шифрует одноразовым message key; nonce нулевой — ключ не живёт
// дольше одного сообщения.
func seal(mk [32]byte, plaintext, aad []byte) []byte {
	aead, err := chacha20poly1305.New(mk[:])
	if err != nil {
		panic("ratchet: aead: " + err.Error())
	}
	nonce := make([]byte, chacha20poly1305.NonceSize)
	return aead.Seal(nil, nonce, plaintext, aad)
}

// open расшифровывает одноразовым message key.
func open(mk [32]byte, ciphertext, aad []byte) ([]byte, error) {
	aead, err := chacha20poly1305.New(mk[:])
	if err != nil {
		panic("ratchet: aead: " + err.Error())
	}
	nonce := make([]byte, chacha20poly1305.NonceSize)
	plain, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, ErrDecrypt
	}
	return plain, nil
}

// headerAAD — канонические байты заголовка для аутентификации: привязка
// сессии плюс (dh_pub, pn, n) в фиксированном порядке.
func headerAAD(ad []byte, dhPub [32]byte, pn, n uint32) []byte {
	out := make([]byte, 0, len(ad)+32+8)
	out = append(out, ad...)
	out = append(out, dhPub[:]...)
	out = append(out,
		byte(pn>>24), byte(pn>>16), byte(pn>>8), byte(pn),
		byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	return out
}

// generateKey — свежая X25519-пара из r.
func generateKey(r io.Reader) (*ecdh.PrivateKey, error) {
	key, err := ecdh.X25519().GenerateKey(r)
	if err != nil {
		return nil, fmt.Errorf("ratchet: генерация ключа: %w", err)
	}
	return key, nil
}
