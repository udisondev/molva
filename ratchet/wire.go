package ratchet

import (
	"errors"
	"fmt"

	"github.com/udisondev/molva/proto/ratchetpb"
	"google.golang.org/protobuf/proto"
)

const (
	// SIDLen — длина идентификатора рукопожатия.
	SIDLen = 16
	// maxCiphertext — потолок шифртекста одного сообщения: plaintext-лимит
	// уровня приложения плюс AEAD-довесок, с запасом под кадр nodenet.
	maxCiphertext = 63 << 10
)

// Ошибки валидации недоверенного входа и протокола.
var (
	ErrMalformed      = errors.New("ratchet: не разбирается")
	ErrDecrypt        = errors.New("ratchet: расшифровка не прошла")
	ErrTooManySkipped = errors.New("ratchet: слишком большая дыра в цепочке")
	ErrNoSendingChain = errors.New("ratchet: отправная цепочка ещё не открыта")
	ErrSIDMismatch    = errors.New("ratchet: ответ на чужое рукопожатие")
)

// Message — сообщение Double Ratchet: открытый заголовок + шифртекст.
type Message struct {
	DHPub      [32]byte
	PN, N      uint32
	Ciphertext []byte
}

// EncodeMessage сериализует сообщение для payload'а конверта.
func EncodeMessage(m Message) ([]byte, error) {
	if len(m.Ciphertext) > maxCiphertext {
		return nil, fmt.Errorf("%w: шифртекст %d байт", ErrMalformed, len(m.Ciphertext))
	}
	return proto.Marshal(&ratchetpb.Message{
		DhPub:      m.DHPub[:],
		Pn:         m.PN,
		N:          m.N,
		Ciphertext: m.Ciphertext,
	})
}

// DecodeMessage разбирает недоверенный вход.
func DecodeMessage(b []byte) (Message, error) {
	var pb ratchetpb.Message
	if err := proto.Unmarshal(b, &pb); err != nil {
		return Message{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if len(pb.DhPub) != 32 || len(pb.Ciphertext) > maxCiphertext {
		return Message{}, ErrMalformed
	}
	m := Message{PN: pb.Pn, N: pb.N, Ciphertext: pb.Ciphertext}
	copy(m.DHPub[:], pb.DhPub)
	return m, nil
}

// Init — первая половина рукопожатия.
type Init struct {
	IK  [32]byte
	Eph [32]byte
	SID [SIDLen]byte
}

// EncodeInit сериализует рукопожатие для payload'а конверта.
func EncodeInit(i Init) ([]byte, error) {
	return proto.Marshal(&ratchetpb.Init{
		IkPub:     i.IK[:],
		EphPub:    i.Eph[:],
		SessionId: i.SID[:],
	})
}

// DecodeInit разбирает недоверенный вход.
func DecodeInit(b []byte) (Init, error) {
	var pb ratchetpb.Init
	if err := proto.Unmarshal(b, &pb); err != nil {
		return Init{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if len(pb.IkPub) != 32 || len(pb.EphPub) != 32 || len(pb.SessionId) != SIDLen {
		return Init{}, ErrMalformed
	}
	var i Init
	copy(i.IK[:], pb.IkPub)
	copy(i.Eph[:], pb.EphPub)
	copy(i.SID[:], pb.SessionId)
	return i, nil
}

// InitAck — ответная половина рукопожатия.
type InitAck struct {
	IK  [32]byte
	Eph [32]byte
	SID [SIDLen]byte
}

// EncodeInitAck сериализует ответ рукопожатия.
func EncodeInitAck(a InitAck) ([]byte, error) {
	return proto.Marshal(&ratchetpb.InitAck{
		IkPub:     a.IK[:],
		EphPub:    a.Eph[:],
		SessionId: a.SID[:],
	})
}

// DecodeInitAck разбирает недоверенный вход.
func DecodeInitAck(b []byte) (InitAck, error) {
	var pb ratchetpb.InitAck
	if err := proto.Unmarshal(b, &pb); err != nil {
		return InitAck{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if len(pb.IkPub) != 32 || len(pb.EphPub) != 32 || len(pb.SessionId) != SIDLen {
		return InitAck{}, ErrMalformed
	}
	var a InitAck
	copy(a.IK[:], pb.IkPub)
	copy(a.Eph[:], pb.EphPub)
	copy(a.SID[:], pb.SessionId)
	return a, nil
}
