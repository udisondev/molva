package ratchet

import (
	"crypto/rand"
	"errors"
	"testing"

	"github.com/udisondev/molva/peer"
)

var (
	idA = peer.ID{0xA1, 1}
	idB = peer.ID{0xB2, 2}
)

// newPair устанавливает сессию рукопожатием: A — инициатор, B — респондент.
func newPair(t *testing.T) (stA, stB *State) {
	t.Helper()
	ikA := IdentityFromSeed([32]byte{1})
	ikB := IdentityFromSeed([32]byte{2})
	h, err := NewHandshake(rand.Reader, ikA)
	if err != nil {
		t.Fatal(err)
	}
	stB, ack, err := AcceptHandshake(rand.Reader, ikB, h.Init(), idA, idB)
	if err != nil {
		t.Fatal(err)
	}
	stA, err = h.Finish(rand.Reader, ack, idA, idB)
	if err != nil {
		t.Fatal(err)
	}
	return stA, stB
}

func mustEncrypt(t *testing.T, s *State, text string) Message {
	t.Helper()
	m, err := s.Encrypt([]byte(text))
	if err != nil {
		t.Fatalf("Encrypt(%q): %v", text, err)
	}
	return m
}

func mustDecrypt(t *testing.T, s *State, m Message) string {
	t.Helper()
	plain, err := s.Decrypt(rand.Reader, m)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	return string(plain)
}

// Многораундовый пинг-понг: обе стороны читают друг друга, DH-ratchet
// крутится на каждом повороте направления.
func TestPingPong(t *testing.T) {
	stA, stB := newPair(t)
	for round := range 5 {
		ma := mustEncrypt(t, stA, "от A")
		if got := mustDecrypt(t, stB, ma); got != "от A" {
			t.Fatalf("раунд %d: %q", round, got)
		}
		mb := mustEncrypt(t, stB, "от B")
		if got := mustDecrypt(t, stA, mb); got != "от B" {
			t.Fatalf("раунд %d: %q", round, got)
		}
	}
}

// Респондент не пишет первым: отправная цепочка открывается только первым
// принятым сообщением.
func TestResponderCannotSendFirst(t *testing.T) {
	stA, stB := newPair(t)
	if _, err := stB.Encrypt([]byte("рано")); !errors.Is(err, ErrNoSendingChain) {
		t.Fatalf("err = %v, want ErrNoSendingChain", err)
	}
	if stB.CanSend() {
		t.Fatal("CanSend до первого приёма")
	}
	m := mustEncrypt(t, stA, "первое")
	mustDecrypt(t, stB, m)
	if !stB.CanSend() {
		t.Fatal("CanSend после первого приёма")
	}
	mustEncrypt(t, stB, "теперь можно")
}

// Внеочередные сообщения одной цепочки: поздние расшифровываются из
// отложенных ключей ровно один раз.
func TestOutOfOrderWithinChain(t *testing.T) {
	stA, stB := newPair(t)
	m0 := mustEncrypt(t, stA, "ноль")
	m1 := mustEncrypt(t, stA, "один")
	m2 := mustEncrypt(t, stA, "два")

	if got := mustDecrypt(t, stB, m2); got != "два" {
		t.Fatalf("m2: %q", got)
	}
	if got := mustDecrypt(t, stB, m0); got != "ноль" {
		t.Fatalf("m0: %q", got)
	}
	if got := mustDecrypt(t, stB, m1); got != "один" {
		t.Fatalf("m1: %q", got)
	}
	// Повтор не проходит: ключ извлечён, replay гибнет.
	if _, err := stB.Decrypt(rand.Reader, m1); err == nil {
		t.Fatal("повторная расшифровка обязана падать")
	}
}

// Дыра через границу DH-шага: непришедший хвост старой цепочки (PN)
// дочитывается из отложенных ключей после ratchet'а.
func TestSkipAcrossRatchet(t *testing.T) {
	stA, stB := newPair(t)
	x1 := mustEncrypt(t, stA, "x1")
	x2 := mustEncrypt(t, stA, "x2") // потеряется до поры

	mustDecrypt(t, stB, x1)
	y1 := mustEncrypt(t, stB, "y1")
	mustDecrypt(t, stA, y1) // у A DH-шаг, новая отправная цепочка

	z1 := mustEncrypt(t, stA, "z1")
	if got := mustDecrypt(t, stB, z1); got != "z1" {
		t.Fatalf("z1: %q", got)
	}
	// Поздний хвост старой цепочки всё ещё читается.
	if got := mustDecrypt(t, stB, x2); got != "x2" {
		t.Fatalf("x2: %q", got)
	}
}

// Слишком большая дыра в одной цепочке отвергается (анти-DoS).
func TestTooManySkipped(t *testing.T) {
	stA, stB := newPair(t)
	// Прокрутить отправную цепочку A далеко вперёд.
	for range MaxSkip + 1 {
		mustEncrypt(t, stA, "пропуск")
	}
	far := mustEncrypt(t, stA, "далёкое")
	if _, err := stB.Decrypt(rand.Reader, far); !errors.Is(err, ErrTooManySkipped) {
		t.Fatalf("err = %v, want ErrTooManySkipped", err)
	}
}

// Forward secrecy: снапшот текущего состояния не читает уже расшифрованное
// прошлое.
func TestForwardSecrecy(t *testing.T) {
	stA, stB := newPair(t)
	old := mustEncrypt(t, stA, "старое письмо")
	mustDecrypt(t, stB, old)
	mb := mustEncrypt(t, stB, "ответ")
	mustDecrypt(t, stA, mb)

	// Компрометация: атакующий получает байты состояния B.
	snap, err := stB.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	fork, err := Unmarshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fork.Decrypt(rand.Reader, old); err == nil {
		t.Fatal("снапшот прочитал прошлое — forward secrecy сломана")
	}
}

// Post-compromise security: после полного раунда пинг-понга форк
// скомпрометированного состояния выпадает из переписки.
func TestPostCompromiseSecurity(t *testing.T) {
	stA, stB := newPair(t)
	mustDecrypt(t, stB, mustEncrypt(t, stA, "разогрев"))
	mustDecrypt(t, stA, mustEncrypt(t, stB, "разогрев-ответ"))

	snap, err := stB.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	fork, err := Unmarshal(snap)
	if err != nil {
		t.Fatal(err)
	}

	// До исцеления: продолжение текущего направления форк ещё читает.
	m1 := mustEncrypt(t, stA, "до исцеления")
	if got := mustDecrypt(t, stB, m1); got != "до исцеления" {
		t.Fatalf("m1: %q", got)
	}
	if _, err := fork.Decrypt(rand.Reader, m1); err != nil {
		t.Fatalf("ожидаемо читаемое до исцеления: %v", err)
	}

	// Полный раунд: B отвечает (свежая пара B), A отвечает (свежая пара A).
	r1 := mustEncrypt(t, stB, "исцеляющий ответ")
	mustDecrypt(t, stA, r1)
	m2 := mustEncrypt(t, stA, "после исцеления")
	if got := mustDecrypt(t, stB, m2); got != "после исцеления" {
		t.Fatalf("m2: %q", got)
	}
	if _, err := fork.Decrypt(rand.Reader, m2); err == nil {
		t.Fatal("форк прочитал сообщение после исцеления — PCS сломана")
	}
}

// Сериализация посреди переписки (с отложенными ключами) продолжает
// сессию без расхождений.
func TestMarshalRoundTripMidConversation(t *testing.T) {
	stA, stB := newPair(t)
	m0 := mustEncrypt(t, stA, "ноль")
	m1 := mustEncrypt(t, stA, "один")
	mustDecrypt(t, stB, m1) // m0 уехал в отложенные

	snap, err := stB.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	stB2, err := Unmarshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustDecrypt(t, stB2, m0); got != "ноль" {
		t.Fatalf("m0 после восстановления: %q", got)
	}
	// Переписка продолжается в обе стороны.
	r := mustEncrypt(t, stB2, "жив")
	if got := mustDecrypt(t, stA, r); got != "жив" {
		t.Fatalf("ответ: %q", got)
	}
}

// Ответ на чужое рукопожатие отвергается.
func TestHandshakeSIDMismatch(t *testing.T) {
	ikA := IdentityFromSeed([32]byte{1})
	ikB := IdentityFromSeed([32]byte{2})
	h, err := NewHandshake(rand.Reader, ikA)
	if err != nil {
		t.Fatal(err)
	}
	_, ack, err := AcceptHandshake(rand.Reader, ikB, h.Init(), idA, idB)
	if err != nil {
		t.Fatal(err)
	}
	ack.SID[0] ^= 0xFF
	if _, err := h.Finish(rand.Reader, ack, idA, idB); !errors.Is(err, ErrSIDMismatch) {
		t.Fatalf("err = %v, want ErrSIDMismatch", err)
	}
}

// Расхождение transcript'ов (подмена NodeID) делает сессии несовместимыми.
func TestTranscriptBindsNodeIDs(t *testing.T) {
	ikA := IdentityFromSeed([32]byte{1})
	ikB := IdentityFromSeed([32]byte{2})
	h, err := NewHandshake(rand.Reader, ikA)
	if err != nil {
		t.Fatal(err)
	}
	stB, ack, err := AcceptHandshake(rand.Reader, ikB, h.Init(), idA, idB)
	if err != nil {
		t.Fatal(err)
	}
	evil := peer.ID{0xEE}
	stA, err := h.Finish(rand.Reader, ack, evil, idB) // инициатор думает о себе иначе
	if err != nil {
		t.Fatal(err)
	}
	m := mustEncrypt(t, stA, "не дойдёт")
	if _, err := stB.Decrypt(rand.Reader, m); err == nil {
		t.Fatal("сессии с разными transcript'ами не должны сходиться")
	}
}

// Восстановленное рукопожатие (рестарт инициатора) завершает сессию.
func TestHandshakePersistence(t *testing.T) {
	ikA := IdentityFromSeed([32]byte{1})
	ikB := IdentityFromSeed([32]byte{2})
	h, err := NewHandshake(rand.Reader, ikA)
	if err != nil {
		t.Fatal(err)
	}
	blob := h.Marshal()

	stB, ack, err := AcceptHandshake(rand.Reader, ikB, h.Init(), idA, idB)
	if err != nil {
		t.Fatal(err)
	}

	h2, err := UnmarshalHandshake(blob, ikA)
	if err != nil {
		t.Fatal(err)
	}
	stA, err := h2.Finish(rand.Reader, ack, idA, idB)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustDecrypt(t, stB, mustEncrypt(t, stA, "после рестарта")); got != "после рестарта" {
		t.Fatalf("%q", got)
	}
}
