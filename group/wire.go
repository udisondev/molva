package group

import (
	"fmt"

	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/proto/grouppb"
	"github.com/udisondev/molva/senderkey"
	"google.golang.org/protobuf/proto"
)

// maxGroupCiphertext — потолок шифртекста группового сообщения.
const maxGroupCiphertext = 33 << 10

// MemberKey — ключ участника в приглашении.
type MemberKey struct {
	Member peer.ID
	Key    senderkey.Dist
}

// Welcome — разобранное приглашение.
type Welcome struct {
	Membership Membership
	Keys       []MemberKey
}

// EncodeWelcome сериализует приглашение.
func EncodeWelcome(w Welcome) ([]byte, error) {
	mpb, err := membershipToPB(w.Membership)
	if err != nil {
		return nil, err
	}
	pb := &grouppb.Welcome{Membership: mpb}
	for _, k := range w.Keys {
		pb.Keys = append(pb.Keys, &grouppb.MemberKey{
			Member: k.Member[:],
			Key:    distToPB(w.Membership.GroupID, k.Key),
		})
	}
	return proto.Marshal(pb)
}

// DecodeWelcome разбирает недоверенное приглашение.
func DecodeWelcome(b []byte) (Welcome, error) {
	var pb grouppb.Welcome
	if err := proto.Unmarshal(b, &pb); err != nil {
		return Welcome{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if pb.Membership == nil || len(pb.Keys) > MaxMembers {
		return Welcome{}, ErrMalformed
	}
	m, err := membershipFromPB(pb.Membership)
	if err != nil {
		return Welcome{}, err
	}
	w := Welcome{Membership: m}
	for _, k := range pb.Keys {
		if k.Key == nil || len(k.Member) != peer.IDLen {
			return Welcome{}, ErrMalformed
		}
		d, err := distFromPB(k.Key)
		if err != nil {
			return Welcome{}, err
		}
		var mk MemberKey
		copy(mk.Member[:], k.Member)
		mk.Key = d
		w.Keys = append(w.Keys, mk)
	}
	return w, nil
}

// EncodeUpdate сериализует новую версию членства.
func EncodeUpdate(m Membership) ([]byte, error) {
	mpb, err := membershipToPB(m)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&grouppb.Update{Membership: mpb})
}

// DecodeUpdate разбирает недоверенное обновление.
func DecodeUpdate(b []byte) (Membership, error) {
	var pb grouppb.Update
	if err := proto.Unmarshal(b, &pb); err != nil {
		return Membership{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if pb.Membership == nil {
		return Membership{}, ErrMalformed
	}
	return membershipFromPB(pb.Membership)
}

// EncodeKeyDist сериализует раздачу sender key.
func EncodeKeyDist(gid [32]byte, d senderkey.Dist) ([]byte, error) {
	return proto.Marshal(distToPB(gid, d))
}

// DecodeKeyDist разбирает недоверенную раздачу.
func DecodeKeyDist(b []byte) (gid [32]byte, d senderkey.Dist, err error) {
	var pb grouppb.SenderKeyDist
	if err := proto.Unmarshal(b, &pb); err != nil {
		return gid, d, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if len(pb.GroupId) != 32 || len(pb.ChainKey) != 32 || len(pb.SignPub) != 32 || pb.Generation == 0 {
		return gid, d, ErrMalformed
	}
	copy(gid[:], pb.GroupId)
	d.Generation = pb.Generation
	d.N = pb.N
	copy(d.ChainKey[:], pb.ChainKey)
	copy(d.SignPub[:], pb.SignPub)
	return gid, d, nil
}

// Msg — групповое сообщение на проводе.
type Msg struct {
	GroupID    [32]byte
	Generation uint32
	N          uint32
	Ciphertext []byte
	Signature  [64]byte
}

// EncodeMsg сериализует групповое сообщение.
func EncodeMsg(m Msg) ([]byte, error) {
	if len(m.Ciphertext) == 0 || len(m.Ciphertext) > maxGroupCiphertext {
		return nil, ErrMalformed
	}
	return proto.Marshal(&grouppb.Message{
		GroupId:    m.GroupID[:],
		Generation: m.Generation,
		N:          m.N,
		Ciphertext: m.Ciphertext,
		Signature:  m.Signature[:],
	})
}

// DecodeMsg разбирает недоверенное групповое сообщение.
func DecodeMsg(b []byte) (Msg, error) {
	var pb grouppb.Message
	if err := proto.Unmarshal(b, &pb); err != nil {
		return Msg{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if len(pb.GroupId) != 32 || len(pb.Signature) != 64 ||
		len(pb.Ciphertext) == 0 || len(pb.Ciphertext) > maxGroupCiphertext || pb.Generation == 0 {
		return Msg{}, ErrMalformed
	}
	var m Msg
	copy(m.GroupID[:], pb.GroupId)
	m.Generation = pb.Generation
	m.N = pb.N
	m.Ciphertext = pb.Ciphertext
	copy(m.Signature[:], pb.Signature)
	return m, nil
}

func membershipToPB(m Membership) (*grouppb.Membership, error) {
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
	return pb, nil
}

func membershipFromPB(pb *grouppb.Membership) (Membership, error) {
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

func distToPB(gid [32]byte, d senderkey.Dist) *grouppb.SenderKeyDist {
	return &grouppb.SenderKeyDist{
		GroupId:    gid[:],
		Generation: d.Generation,
		ChainKey:   d.ChainKey[:],
		N:          d.N,
		SignPub:    d.SignPub[:],
	}
}

func distFromPB(pb *grouppb.SenderKeyDist) (senderkey.Dist, error) {
	if len(pb.ChainKey) != 32 || len(pb.SignPub) != 32 || pb.Generation == 0 {
		return senderkey.Dist{}, ErrMalformed
	}
	var d senderkey.Dist
	d.Generation = pb.Generation
	d.N = pb.N
	copy(d.ChainKey[:], pb.ChainKey)
	copy(d.SignPub[:], pb.SignPub)
	return d, nil
}
