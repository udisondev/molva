// Package group — групповые чаты molva: членство с подписью админа,
// sender keys по попарным DR-сессиям, веерная рассылка через общий
// надёжный outbox, обязательный rekey на удаление участника.
package group

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"slices"
	"sort"
	"unicode/utf8"

	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/proto/grouppb"
	"golang.org/x/crypto/blake2b"
	"google.golang.org/protobuf/proto"
)

const (
	// MaxMembers — потолок размера группы v1 (цена fan-out).
	MaxMembers = 128
	// maxGroupName — потолок имени группы в рунах.
	maxGroupName = 64

	labelMembership = "molva/group/membership/v1"
)

// Ошибки членства.
var (
	ErrMalformed    = errors.New("group: не разбирается")
	ErrBadSignature = errors.New("group: подпись членства не сходится")
	ErrNotAdmin     = errors.New("group: операция доступна только админу")
	ErrNotMember    = errors.New("group: пир не состоит в группе")
	ErrTooBig       = errors.New("group: превышен размер группы")
	ErrUnknown      = errors.New("group: группа неизвестна")
	ErrLeft         = errors.New("group: вы больше не в группе")
)

// Membership — разобранный документ членства.
type Membership struct {
	GroupID  [32]byte
	Version  uint64
	Name     string
	AdminPub [32]byte
	Members  []peer.ID // отсортированы байтово
	Sig      [64]byte
}

// transcript — каноничные байты под подпись (protobuf не каноничен).
func (m *Membership) transcript() []byte {
	h, err := blake2b.New256(nil)
	if err != nil {
		panic("group: transcript: " + err.Error())
	}
	h.Write([]byte(labelMembership))
	h.Write(m.GroupID[:])
	var v [8]byte
	for i := range 8 {
		v[i] = byte(m.Version >> (56 - 8*i))
	}
	h.Write(v[:])
	h.Write([]byte(m.Name))
	h.Write(m.AdminPub[:])
	for _, p := range m.Members {
		h.Write(p[:])
	}
	return h.Sum(nil)
}

// Sign подписывает документ админским ключом (члены сортируются).
func (m *Membership) Sign(priv ed25519.PrivateKey) {
	sortMembers(m.Members)
	copy(m.Sig[:], ed25519.Sign(priv, m.transcript()))
}

// Verify проверяет подпись против admin_pub документа.
func (m *Membership) Verify() bool {
	return ed25519.Verify(m.AdminPub[:], m.transcript(), m.Sig[:])
}

// Has — состоит ли пир в документе.
func (m *Membership) Has(p peer.ID) bool { return slices.Contains(m.Members, p) }

func sortMembers(members []peer.ID) {
	sort.Slice(members, func(i, j int) bool {
		return bytes.Compare(members[i][:], members[j][:]) < 0
	})
}

// EncodeMembership сериализует документ.
func EncodeMembership(m Membership) ([]byte, error) {
	if err := validateMembership(&m); err != nil {
		return nil, err
	}
	pb := &grouppb.Membership{
		GroupId:   m.GroupID[:],
		Version:   m.Version,
		Name:      m.Name,
		AdminPub:  m.AdminPub[:],
		Signature: m.Sig[:],
	}
	for _, p := range m.Members {
		pb.Members = append(pb.Members, p[:])
	}
	return proto.Marshal(pb)
}

// DecodeMembership разбирает недоверенный документ (подпись проверяет
// вызывающий — против известного ему admin_pub).
func DecodeMembership(b []byte) (Membership, error) {
	var pb grouppb.Membership
	if err := proto.Unmarshal(b, &pb); err != nil {
		return Membership{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if len(pb.GroupId) != 32 || len(pb.AdminPub) != 32 || len(pb.Signature) != 64 {
		return Membership{}, ErrMalformed
	}
	m := Membership{Version: pb.Version, Name: pb.Name}
	copy(m.GroupID[:], pb.GroupId)
	copy(m.AdminPub[:], pb.AdminPub)
	copy(m.Sig[:], pb.Signature)
	for _, mb := range pb.Members {
		if len(mb) != peer.IDLen {
			return Membership{}, ErrMalformed
		}
		var p peer.ID
		copy(p[:], mb)
		m.Members = append(m.Members, p)
	}
	if err := validateMembership(&m); err != nil {
		return Membership{}, err
	}
	return m, nil
}

func validateMembership(m *Membership) error {
	if m.Version == 0 {
		return fmt.Errorf("%w: нулевая версия", ErrMalformed)
	}
	if len(m.Members) == 0 || len(m.Members) > MaxMembers {
		return fmt.Errorf("%w: %d участников", ErrMalformed, len(m.Members))
	}
	if !utf8.ValidString(m.Name) || utf8.RuneCountInString(m.Name) > maxGroupName {
		return fmt.Errorf("%w: имя", ErrMalformed)
	}
	return nil
}
