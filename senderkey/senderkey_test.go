package senderkey

import (
	"crypto/rand"
	"errors"
	"testing"
)

var gid = [32]byte{0x47, 1, 2}

func newPair(t *testing.T) (*Sender, *Receiver) {
	t.Helper()
	s, err := NewSender(rand.Reader, 1)
	if err != nil {
		t.Fatal(err)
	}
	return s, NewReceiver(s.Dist())
}

func TestRoundTripAndOrder(t *testing.T) {
	s, r := newPair(t)
	for i := range 5 {
		n, ct, sig := s.Encrypt(gid, []byte("привет"))
		if int(n) != i {
			t.Fatalf("n = %d, want %d", n, i)
		}
		plain, err := r.Decrypt(gid, 1, n, ct, sig)
		if err != nil || string(plain) != "привет" {
			t.Fatalf("decrypt: %q %v", plain, err)
		}
	}
}

func TestOutOfOrderAndReplay(t *testing.T) {
	s, r := newPair(t)
	n0, ct0, sig0 := s.Encrypt(gid, []byte("ноль"))
	n1, ct1, sig1 := s.Encrypt(gid, []byte("один"))
	n2, ct2, sig2 := s.Encrypt(gid, []byte("два"))

	if got, err := r.Decrypt(gid, 1, n2, ct2, sig2); err != nil || string(got) != "два" {
		t.Fatalf("m2: %q %v", got, err)
	}
	if got, err := r.Decrypt(gid, 1, n0, ct0, sig0); err != nil || string(got) != "ноль" {
		t.Fatalf("m0: %q %v", got, err)
	}
	// Повтор гибнет: ключ извлечён.
	if _, err := r.Decrypt(gid, 1, n0, ct0, sig0); !errors.Is(err, ErrOldMessage) {
		t.Fatalf("replay: %v", err)
	}
	if got, err := r.Decrypt(gid, 1, n1, ct1, sig1); err != nil || string(got) != "один" {
		t.Fatalf("m1: %q %v", got, err)
	}
}

// Новичок получает текущую точку цепочки и не читает историю.
func TestNewcomerCannotReadHistory(t *testing.T) {
	s, _ := newPair(t)
	nOld, ctOld, sigOld := s.Encrypt(gid, []byte("до вступления"))

	late := NewReceiver(s.Dist()) // раздача после первого сообщения
	if _, err := late.Decrypt(gid, 1, nOld, ctOld, sigOld); !errors.Is(err, ErrOldMessage) {
		t.Fatalf("новичок прочитал историю: %v", err)
	}
	nNew, ctNew, sigNew := s.Encrypt(gid, []byte("после"))
	if got, err := late.Decrypt(gid, 1, nNew, ctNew, sigNew); err != nil || string(got) != "после" {
		t.Fatalf("новое: %q %v", got, err)
	}
}

// Подпись обязательна: участник со знанием всех ключей не может писать от
// чужого имени — приёмник сверяет подпись с sign_pub владельца.
func TestForgedSignatureRejected(t *testing.T) {
	_, r := newPair(t) // r ждёт сообщений владельца s
	evil, err := NewSender(rand.Reader, 1)
	if err != nil {
		t.Fatal(err)
	}
	n, ct, sig := evil.Encrypt(gid, []byte("подделка от чужого имени"))
	if _, err := r.Decrypt(gid, 1, n, ct, sig); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("подделка прошла: %v", err)
	}
}

// Старое поколение после rekey не читается.
func TestRekeyGeneration(t *testing.T) {
	s, r := newPair(t)
	n, ct, sig := s.Encrypt(gid, []byte("до rekey"))
	if _, err := r.Decrypt(gid, 1, n, ct, sig); err != nil {
		t.Fatal(err)
	}

	s2, err := NewSender(rand.Reader, 2)
	if err != nil {
		t.Fatal(err)
	}
	r2 := NewReceiver(s2.Dist())
	n2, ct2, sig2 := s2.Encrypt(gid, []byte("после rekey"))
	// Приёмник старого поколения сигналит «ключ ещё едет» — доставка
	// переигрывается, когда rekey доедет.
	if _, err := r.Decrypt(gid, 2, n2, ct2, sig2); !errors.Is(err, ErrFutureKey) {
		t.Fatalf("кросс-поколение: %v", err)
	}
	if got, err := r2.Decrypt(gid, 2, n2, ct2, sig2); err != nil || string(got) != "после rekey" {
		t.Fatalf("новое поколение: %q %v", got, err)
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	s, r := newPair(t)
	// Дыра, чтобы появились отложенные ключи.
	_, _, _ = s.Encrypt(gid, []byte("x0"))
	n1, ct1, sig1 := s.Encrypt(gid, []byte("x1"))
	if _, err := r.Decrypt(gid, 1, n1, ct1, sig1); err != nil {
		t.Fatal(err)
	}

	s2, err := UnmarshalSender(s.Marshal())
	if err != nil {
		t.Fatal(err)
	}
	r2, err := UnmarshalReceiver(r.Marshal())
	if err != nil {
		t.Fatal(err)
	}
	n2, ct2, sig2 := s2.Encrypt(gid, []byte("x2"))
	if got, err := r2.Decrypt(gid, 1, n2, ct2, sig2); err != nil || string(got) != "x2" {
		t.Fatalf("после восстановления: %q %v", got, err)
	}
}

// FuzzUnmarshalReceiver: байты из store повреждены — не паникуем.
func FuzzUnmarshalReceiver(f *testing.F) {
	f.Add([]byte{})
	s, _ := NewSender(rand.Reader, 1)
	r := NewReceiver(s.Dist())
	f.Add(r.Marshal())
	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := UnmarshalReceiver(data)
		if err != nil {
			return
		}
		_ = r.Marshal()
	})
}

// FuzzUnmarshalSender: аналогично для собственного ключа.
func FuzzUnmarshalSender(f *testing.F) {
	f.Add([]byte{})
	s, _ := NewSender(rand.Reader, 1)
	f.Add(s.Marshal())
	f.Fuzz(func(t *testing.T, data []byte) {
		s, err := UnmarshalSender(data)
		if err != nil {
			return
		}
		_ = s.Marshal()
	})
}
