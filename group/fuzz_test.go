package group

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/senderkey"
)

func sampleMembership(t testing.TB) ([]byte, ed25519.PrivateKey) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	m := Membership{GroupID: [32]byte{1}, Version: 2, Name: "кружок", Members: []peer.ID{{1}, {2}}}
	copy(m.AdminPub[:], priv.Public().(ed25519.PublicKey))
	m.Sign(priv)
	b, err := EncodeMembership(m)
	if err != nil {
		t.Fatal(err)
	}
	return b, priv
}

func TestMembershipSignVerify(t *testing.T) {
	b, _ := sampleMembership(t)
	m, err := DecodeMembership(b)
	if err != nil {
		t.Fatal(err)
	}
	if !m.Verify() {
		t.Fatal("подпись обязана сходиться")
	}
	m.Name = "подмена"
	if m.Verify() {
		t.Fatal("подмена поля пережила подпись")
	}
}

// FuzzDecodeMembership: документ приходит от пира — произвольные байты не
// паникуют, успех переживает round-trip.
func FuzzDecodeMembership(f *testing.F) {
	f.Add([]byte{})
	valid, _ := sampleMembership(f)
	f.Add(valid)
	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := DecodeMembership(data)
		if err != nil {
			return
		}
		b, err := EncodeMembership(m)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		m2, err := DecodeMembership(b)
		if err != nil {
			t.Fatalf("re-decode: %v", err)
		}
		if m2.GroupID != m.GroupID || m2.Version != m.Version || len(m2.Members) != len(m.Members) {
			t.Fatal("round-trip разошёлся")
		}
	})
}

// FuzzDecodeWelcome: приглашение — недоверенный вход.
func FuzzDecodeWelcome(f *testing.F) {
	f.Add([]byte{})
	memB, _ := sampleMembership(f)
	mem, _ := DecodeMembership(memB)
	s, _ := senderkey.NewSender(rand.Reader, 1)
	valid, _ := EncodeWelcome(Welcome{
		Membership: mem,
		Keys:       []MemberKey{{Member: peer.ID{1}, Key: s.Dist()}},
	})
	f.Add(valid)
	f.Fuzz(func(t *testing.T, data []byte) {
		w, err := DecodeWelcome(data)
		if err != nil {
			return
		}
		if len(w.Keys) > MaxMembers || len(w.Membership.Members) > MaxMembers {
			t.Fatal("валидация пропустила превышение размеров")
		}
	})
}

// FuzzDecodeMsg: групповое сообщение — недоверенный вход.
func FuzzDecodeMsg(f *testing.F) {
	f.Add([]byte{})
	valid, _ := EncodeMsg(Msg{GroupID: [32]byte{1}, Generation: 1, N: 0, Ciphertext: []byte("ct")})
	f.Add(valid)
	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := DecodeMsg(data)
		if err != nil {
			return
		}
		if len(m.Ciphertext) == 0 || len(m.Ciphertext) > maxGroupCiphertext || m.Generation == 0 {
			t.Fatal("валидация пропустила мусор")
		}
	})
}
