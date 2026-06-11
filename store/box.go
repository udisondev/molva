package store

import (
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
	"golang.org/x/crypto/chacha20poly1305"
)

// ErrWrongKey — содержимое не расшифровывается этим ключом: БД создана
// другим seed'ом либо повреждена.
var ErrWrongKey = errors.New("store: ключ не подходит к базе")

// box — шифрование контент-полей: XChaCha20-Poly1305, случайный nonce на
// запись (24 байта nonce исключают коллизии при любом числе записей),
// AAD привязывает шифртекст к месту хранения — подмена записи местами
// не пройдёт расшифровку.
type box struct {
	aead cipher.AEAD
}

func newBox(key [32]byte) (box, error) {
	aead, err := chacha20poly1305.NewX(key[:])
	if err != nil {
		return box{}, fmt.Errorf("store: aead: %w", err)
	}
	return box{aead: aead}, nil
}

// seal шифрует plain с привязкой к aad; результат — nonce||ciphertext.
func (b box) seal(plain, aad []byte) ([]byte, error) {
	out := make([]byte, chacha20poly1305.NonceSizeX, chacha20poly1305.NonceSizeX+len(plain)+b.aead.Overhead())
	if _, err := rand.Read(out[:chacha20poly1305.NonceSizeX]); err != nil {
		return nil, fmt.Errorf("store: nonce: %w", err)
	}
	return b.aead.Seal(out, out[:chacha20poly1305.NonceSizeX], plain, aad), nil
}

// open расшифровывает blob вида nonce||ciphertext, проверяя привязку к aad.
func (b box) open(blob, aad []byte) ([]byte, error) {
	if len(blob) < chacha20poly1305.NonceSizeX+b.aead.Overhead() {
		return nil, ErrWrongKey
	}
	plain, err := b.aead.Open(nil, blob[:chacha20poly1305.NonceSizeX], blob[chacha20poly1305.NonceSizeX:], aad)
	if err != nil {
		return nil, ErrWrongKey
	}
	return plain, nil
}

// AAD-конструкторы: каждый шифртекст привязан к таблице, полю и ключу
// записи — записи нельзя переставить местами незаметно.

func aadMessage(p peer.ID, mid envelope.MsgID, outgoing bool) []byte {
	out := make([]byte, 0, len("messages.body")+peer.IDLen+envelope.MsgIDLen+1)
	out = append(out, "messages.body"...)
	out = append(out, p[:]...)
	out = append(out, mid[:]...)
	if outgoing {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	return out
}

func aadOutbox(p peer.ID, mid envelope.MsgID) []byte {
	out := make([]byte, 0, len("outbox.frame")+peer.IDLen+envelope.MsgIDLen)
	out = append(out, "outbox.frame"...)
	out = append(out, p[:]...)
	out = append(out, mid[:]...)
	return out
}

func aadMeta(k string) []byte {
	return append([]byte("meta."), k...)
}
