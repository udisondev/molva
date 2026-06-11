package senderkey

import (
	"crypto/ed25519"
	"encoding/binary"
	"io"
)

// Sender — собственный sender key на группу: отправная цепочка и ключ
// подписи. Использование как у ratchet.State: загрузить, применить,
// сохранить в одной транзакции.
type Sender struct {
	generation uint32
	ck         [32]byte
	n          uint32
	signPriv   ed25519.PrivateKey
}

// NewSender — свежий ключ поколения generation (растёт при rekey).
func NewSender(r io.Reader, generation uint32) (*Sender, error) {
	ck, err := readKey(r)
	if err != nil {
		return nil, err
	}
	_, priv, err := ed25519.GenerateKey(r)
	if err != nil {
		return nil, err
	}
	return &Sender{generation: generation, ck: ck, signPriv: priv}, nil
}

// Generation — поколение ключа.
func (s *Sender) Generation() uint32 { return s.generation }

// Dist — текущая точка цепочки для раздачи: получивший прочтёт всё
// с этого места и ничего до него.
func (s *Sender) Dist() Dist {
	d := Dist{Generation: s.generation, ChainKey: s.ck, N: s.n}
	copy(d.SignPub[:], s.signPriv.Public().(ed25519.PublicKey))
	return d
}

// Encrypt шифрует сообщение очередным шагом цепочки и подписывает его.
func (s *Sender) Encrypt(groupID [32]byte, plaintext []byte) (n uint32, ciphertext, signature []byte) {
	mk, next := kdfChain(s.ck)
	n = s.n
	ciphertext = seal(mk, groupID, n, plaintext)
	signature = ed25519.Sign(s.signPriv, signTranscript(groupID, s.generation, n, ciphertext))
	s.ck = next
	s.n++
	return n, ciphertext, signature
}

// Marshal — сериализация для шифрованного хранения.
func (s *Sender) Marshal() []byte {
	out := make([]byte, 0, 8+32+ed25519.PrivateKeySize)
	out = binary.BigEndian.AppendUint32(out, s.generation)
	out = binary.BigEndian.AppendUint32(out, s.n)
	out = append(out, s.ck[:]...)
	out = append(out, s.signPriv...)
	return out
}

// UnmarshalSender восстанавливает собственный ключ.
func UnmarshalSender(b []byte) (*Sender, error) {
	if len(b) != 8+32+ed25519.PrivateKeySize {
		return nil, ErrMalformed
	}
	s := &Sender{
		generation: binary.BigEndian.Uint32(b),
		n:          binary.BigEndian.Uint32(b[4:]),
		signPriv:   ed25519.PrivateKey(append([]byte(nil), b[40:]...)),
	}
	copy(s.ck[:], b[8:40])
	return s, nil
}
