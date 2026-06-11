package ratchet

import (
	"crypto/ecdh"
	"io"

	"github.com/udisondev/molva/proto/ratchetpb"
	"google.golang.org/protobuf/proto"
)

// MaxSkip — потолок отложенных ключей: и на одну дыру, и на сессию
// (анти-DoS памяти и стора; при переполнении старейшие вытесняются —
// их сообщения уже не расшифровать).
const MaxSkip = 512

// skippedEntry — отложенный ключ внеочередного сообщения; порядок среза —
// порядок появления (для вытеснения старейших).
type skippedEntry struct {
	dh [32]byte
	n  uint32
	mk [32]byte
}

// State — состояние одной сессии Double Ratchet. Не потокобезопасно и не
// предназначено жить в памяти: загрузить из store, применить одну операцию,
// сохранить в той же транзакции, объект выбросить. При ошибке Decrypt
// объект обязателен к выбросу без сохранения.
type State struct {
	rk             [32]byte
	dhs            *ecdh.PrivateKey
	dhr            []byte // 32 байта; nil у респондента до первого приёма
	cks, ckr       [32]byte
	hasCKs, hasCKr bool
	ns, nr, pn     uint32
	ad             []byte
	skipped        []skippedEntry
}

// Encrypt шифрует очередное исходящее, продвигая отправную цепочку.
func (s *State) Encrypt(plaintext []byte) (Message, error) {
	if !s.hasCKs {
		return Message{}, ErrNoSendingChain
	}
	mk, next := kdfChain(s.cks)
	s.cks = next

	var m Message
	copy(m.DHPub[:], s.dhs.PublicKey().Bytes())
	m.PN, m.N = s.pn, s.ns
	m.Ciphertext = seal(mk, plaintext, headerAAD(s.ad, m.DHPub, m.PN, m.N))
	s.ns++
	return m, nil
}

// Decrypt расшифровывает входящее, продвигая приёмную цепочку (и DH-ratchet
// при новом ключе пира; r — источник свежей пары). Ошибка означает, что
// состояние объекта испорчено: не сохранять, перечитать из store.
func (s *State) Decrypt(r io.Reader, m Message) ([]byte, error) {
	if mk, ok := s.takeSkipped(m.DHPub, m.N); ok {
		return open(mk, m.Ciphertext, headerAAD(s.ad, m.DHPub, m.PN, m.N))
	}
	if s.dhr == nil || m.DHPub != [32]byte(s.dhr) {
		// Новый ratchet-ключ пира: дозакрыть старую приёмную цепочку до
		// заявленной длины, затем шаг DH-ratchet.
		if s.hasCKr {
			if err := s.skipTo(m.PN); err != nil {
				return nil, err
			}
		}
		if err := s.dhRatchet(r, m.DHPub); err != nil {
			return nil, err
		}
	}
	if err := s.skipTo(m.N); err != nil {
		return nil, err
	}
	mk, next := kdfChain(s.ckr)
	plain, err := open(mk, m.Ciphertext, headerAAD(s.ad, m.DHPub, m.PN, m.N))
	if err != nil {
		return nil, err
	}
	s.ckr = next
	s.nr++
	return plain, nil
}

// takeSkipped ищет отложенный ключ; найденный извлекается.
func (s *State) takeSkipped(dh [32]byte, n uint32) ([32]byte, bool) {
	for i, e := range s.skipped {
		if e.dh == dh && e.n == n {
			s.skipped = append(s.skipped[:i], s.skipped[i+1:]...)
			return e.mk, true
		}
	}
	return [32]byte{}, false
}

// skipTo продвигает приёмную цепочку до номера until, откладывая ключи
// пропущенных сообщений.
func (s *State) skipTo(until uint32) error {
	if !s.hasCKr {
		return nil
	}
	if until > s.nr && until-s.nr > MaxSkip {
		return ErrTooManySkipped
	}
	var dh [32]byte
	copy(dh[:], s.dhr)
	for s.nr < until {
		mk, next := kdfChain(s.ckr)
		s.skipped = append(s.skipped, skippedEntry{dh: dh, n: s.nr, mk: mk})
		if len(s.skipped) > MaxSkip {
			s.skipped = s.skipped[1:]
		}
		s.ckr = next
		s.nr++
	}
	return nil
}

// dhRatchet — шаг DH-ratchet'а по новому ключу пира: закрывается отправная
// цепочка, открываются свежие приёмная и отправная.
func (s *State) dhRatchet(r io.Reader, newDHr [32]byte) error {
	pub, err := ecdh.X25519().NewPublicKey(newDHr[:])
	if err != nil {
		return ErrMalformed
	}
	s.pn = s.ns
	s.ns, s.nr = 0, 0
	s.dhr = newDHr[:]

	dhOut, err := s.dhs.ECDH(pub)
	if err != nil {
		return ErrMalformed
	}
	s.rk, s.ckr = kdfRoot(s.rk, dhOut)
	s.hasCKr = true

	dhs, err := generateKey(r)
	if err != nil {
		return err
	}
	s.dhs = dhs
	dhOut, err = s.dhs.ECDH(pub)
	if err != nil {
		return ErrMalformed
	}
	s.rk, s.cks = kdfRoot(s.rk, dhOut)
	s.hasCKs = true
	return nil
}

// Marshal сериализует состояние. Внутри приватные ключи и цепочки —
// хранить только шифрованным (store сделает это сам).
func (s *State) Marshal() ([]byte, error) {
	pb := &ratchetpb.State{
		Rk:      s.rk[:],
		DhsPriv: s.dhs.Bytes(),
		DhrPub:  s.dhr,
		Ns:      s.ns,
		Nr:      s.nr,
		Pn:      s.pn,
		Ad:      s.ad,
	}
	if s.hasCKs {
		pb.Cks = s.cks[:]
	}
	if s.hasCKr {
		pb.Ckr = s.ckr[:]
	}
	for _, e := range s.skipped {
		pb.Skipped = append(pb.Skipped, &ratchetpb.SkippedKey{
			DhPub: e.dh[:], N: e.n, Mk: e.mk[:],
		})
	}
	return proto.Marshal(pb)
}

// Unmarshal восстанавливает состояние из сериализации Marshal.
func Unmarshal(b []byte) (*State, error) {
	var pb ratchetpb.State
	if err := proto.Unmarshal(b, &pb); err != nil {
		return nil, ErrMalformed
	}
	if len(pb.Rk) != 32 || len(pb.DhsPriv) != 32 {
		return nil, ErrMalformed
	}
	if pb.DhrPub != nil && len(pb.DhrPub) != 32 {
		return nil, ErrMalformed
	}
	if (pb.Cks != nil && len(pb.Cks) != 32) || (pb.Ckr != nil && len(pb.Ckr) != 32) {
		return nil, ErrMalformed
	}
	dhs, err := ecdh.X25519().NewPrivateKey(pb.DhsPriv)
	if err != nil {
		return nil, ErrMalformed
	}
	s := &State{
		dhs: dhs,
		dhr: pb.DhrPub,
		ns:  pb.Ns,
		nr:  pb.Nr,
		pn:  pb.Pn,
		ad:  pb.Ad,
	}
	copy(s.rk[:], pb.Rk)
	if pb.Cks != nil {
		copy(s.cks[:], pb.Cks)
		s.hasCKs = true
	}
	if pb.Ckr != nil {
		copy(s.ckr[:], pb.Ckr)
		s.hasCKr = true
	}
	if len(pb.Skipped) > MaxSkip {
		return nil, ErrMalformed
	}
	for _, e := range pb.Skipped {
		if len(e.DhPub) != 32 || len(e.Mk) != 32 {
			return nil, ErrMalformed
		}
		var entry skippedEntry
		copy(entry.dh[:], e.DhPub)
		copy(entry.mk[:], e.Mk)
		entry.n = e.N
		s.skipped = append(s.skipped, entry)
	}
	return s, nil
}

// CanSend — открыта ли отправная цепочка (у респондента — после первого
// принятого сообщения).
func (s *State) CanSend() bool { return s.hasCKs }
