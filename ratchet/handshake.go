package ratchet

import (
	"crypto/ecdh"
	"fmt"
	"io"

	"github.com/udisondev/molva/peer"
)

// Handshake — состояние инициатора между Init и InitAck. Переживает
// рестарт через Marshal/UnmarshalHandshake (хранить только шифрованным:
// внутри приватный эфемерный ключ).
type Handshake struct {
	ik  *ecdh.PrivateKey
	eph *ecdh.PrivateKey
	sid [SIDLen]byte
}

// NewHandshake начинает рукопожатие: свежий эфемерный ключ и session id.
func NewHandshake(r io.Reader, ik *ecdh.PrivateKey) (*Handshake, error) {
	eph, err := generateKey(r)
	if err != nil {
		return nil, err
	}
	var sid [SIDLen]byte
	if _, err := io.ReadFull(r, sid[:]); err != nil {
		return nil, fmt.Errorf("ratchet: session id: %w", err)
	}
	return &Handshake{ik: ik, eph: eph, sid: sid}, nil
}

// Init — payload первой половины рукопожатия.
func (h *Handshake) Init() Init {
	var i Init
	copy(i.IK[:], h.ik.PublicKey().Bytes())
	copy(i.Eph[:], h.eph.PublicKey().Bytes())
	i.SID = h.sid
	return i
}

// SID — идентификатор этого рукопожатия.
func (h *Handshake) SID() [SIDLen]byte { return h.sid }

// Finish завершает рукопожатие инициатора по ответу респондента и отдаёт
// готовое состояние сессии. self — наш NodeID, responder — его.
func (h *Handshake) Finish(r io.Reader, ack InitAck, self, responder peer.ID) (*State, error) {
	if ack.SID != h.sid {
		return nil, ErrSIDMismatch
	}
	ikR, err := ecdh.X25519().NewPublicKey(ack.IK[:])
	if err != nil {
		return nil, fmt.Errorf("%w: ik ответа", ErrMalformed)
	}
	ephR, err := ecdh.X25519().NewPublicKey(ack.Eph[:])
	if err != nil {
		return nil, fmt.Errorf("%w: eph ответа", ErrMalformed)
	}
	// dh1 = DH(IK_иниц, eph_респ); dh2 = DH(eph_иниц, IK_респ);
	// dh3 = DH(eph_иниц, eph_респ) — у респондента те же три значения.
	dh1, err := h.ik.ECDH(ephR)
	if err != nil {
		return nil, fmt.Errorf("%w: dh1", ErrMalformed)
	}
	dh2, err := h.eph.ECDH(ikR)
	if err != nil {
		return nil, fmt.Errorf("%w: dh2", ErrMalformed)
	}
	dh3, err := h.eph.ECDH(ephR)
	if err != nil {
		return nil, fmt.Errorf("%w: dh3", ErrMalformed)
	}
	init := h.Init()
	tr := transcript(self, responder, init.IK[:], ack.IK[:], init.Eph[:], ack.Eph[:], h.sid)
	sk := deriveSK(dh1, dh2, dh3, tr)

	// Инициатор: эфемерный ключ респондента — его стартовый ratchet-ключ;
	// своя пара — свежая, первый DH-шаг открывает отправную цепочку.
	dhs, err := generateKey(r)
	if err != nil {
		return nil, err
	}
	dhOut, err := dhs.ECDH(ephR)
	if err != nil {
		return nil, fmt.Errorf("%w: dh ratchet", ErrMalformed)
	}
	rk, cks := kdfRoot(sk, dhOut)
	return &State{
		rk:     rk,
		dhs:    dhs,
		dhr:    ack.Eph[:],
		cks:    cks,
		hasCKs: true,
		ad:     tr,
	}, nil
}

// Marshal — персист незавершённого рукопожатия: eph_priv || sid.
func (h *Handshake) Marshal() []byte {
	out := make([]byte, 0, 32+SIDLen)
	out = append(out, h.eph.Bytes()...)
	out = append(out, h.sid[:]...)
	return out
}

// UnmarshalHandshake восстанавливает рукопожатие; ik подаёт вызывающий
// (он выводится из seed и не хранится).
func UnmarshalHandshake(b []byte, ik *ecdh.PrivateKey) (*Handshake, error) {
	if len(b) != 32+SIDLen {
		return nil, ErrMalformed
	}
	eph, err := ecdh.X25519().NewPrivateKey(b[:32])
	if err != nil {
		return nil, ErrMalformed
	}
	h := &Handshake{ik: ik, eph: eph}
	copy(h.sid[:], b[32:])
	return h, nil
}

// AcceptHandshake — сторона респондента: по Init строит состояние сессии и
// ответ. initiator — NodeID инициатора, self — наш.
func AcceptHandshake(r io.Reader, ik *ecdh.PrivateKey, init Init, initiator, self peer.ID) (*State, InitAck, error) {
	ikI, err := ecdh.X25519().NewPublicKey(init.IK[:])
	if err != nil {
		return nil, InitAck{}, fmt.Errorf("%w: ik инициатора", ErrMalformed)
	}
	ephI, err := ecdh.X25519().NewPublicKey(init.Eph[:])
	if err != nil {
		return nil, InitAck{}, fmt.Errorf("%w: eph инициатора", ErrMalformed)
	}
	eph, err := generateKey(r)
	if err != nil {
		return nil, InitAck{}, err
	}
	dh1, err := eph.ECDH(ikI)
	if err != nil {
		return nil, InitAck{}, fmt.Errorf("%w: dh1", ErrMalformed)
	}
	dh2, err := ik.ECDH(ephI)
	if err != nil {
		return nil, InitAck{}, fmt.Errorf("%w: dh2", ErrMalformed)
	}
	dh3, err := eph.ECDH(ephI)
	if err != nil {
		return nil, InitAck{}, fmt.Errorf("%w: dh3", ErrMalformed)
	}
	var ack InitAck
	copy(ack.IK[:], ik.PublicKey().Bytes())
	copy(ack.Eph[:], eph.PublicKey().Bytes())
	ack.SID = init.SID

	tr := transcript(initiator, self, init.IK[:], ack.IK[:], init.Eph[:], ack.Eph[:], init.SID)
	sk := deriveSK(dh1, dh2, dh3, tr)

	// Респондент: его эфемерная пара — стартовая ratchet-пара; цепочки
	// откроются первым принятым сообщением (до этого писать нельзя —
	// хочешь писать первым, инициируй рукопожатие сам).
	return &State{
		rk:  sk,
		dhs: eph,
		ad:  tr,
	}, ack, nil
}
