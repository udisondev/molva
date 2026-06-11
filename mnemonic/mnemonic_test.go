package mnemonic

import (
	"crypto/rand"
	"errors"
	"strings"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	for range 100 {
		var seed [32]byte
		if _, err := rand.Read(seed[:]); err != nil {
			t.Fatal(err)
		}
		phrase := FromSeed(seed)
		if len(strings.Fields(phrase)) != Words {
			t.Fatalf("слов %d, want %d", len(strings.Fields(phrase)), Words)
		}
		got, err := ToSeed(phrase)
		if err != nil {
			t.Fatalf("ToSeed: %v", err)
		}
		if got != seed {
			t.Fatalf("round-trip разошёлся: %x != %x", got[:4], seed[:4])
		}
	}
}

// Известный вектор BIP39: нулевая энтропия → фраза из 24 "abandon" +
// "art" (стандартный тест-вектор).
func TestKnownVector(t *testing.T) {
	phrase := FromSeed([32]byte{})
	want := strings.Repeat("abandon ", 23) + "art"
	if phrase != want {
		t.Fatalf("нулевой seed:\n got %q\nwant %q", phrase, want)
	}
	seed, err := ToSeed(want)
	if err != nil || seed != [32]byte{} {
		t.Fatalf("обратно: %x %v", seed, err)
	}
}

func TestNormalization(t *testing.T) {
	seed := [32]byte{1, 2, 3}
	phrase := FromSeed(seed)
	// Разный регистр и лишние пробелы — должны разбираться.
	messy := "  " + strings.ReplaceAll(strings.ToUpper(phrase), " ", "   ") + "  "
	got, err := ToSeed(messy)
	if err != nil || got != seed {
		t.Fatalf("нормализация: %x %v", got, err)
	}
}

func TestRejects(t *testing.T) {
	good := FromSeed([32]byte{7})
	words := strings.Fields(good)

	if _, err := ToSeed("слишком коротко"); !errors.Is(err, ErrWordCount) {
		t.Fatalf("короткая: %v", err)
	}
	if _, err := ToSeed(good + " extra"); !errors.Is(err, ErrWordCount) {
		t.Fatalf("длинная: %v", err)
	}

	// Неизвестное слово.
	bad := append([]string(nil), words...)
	bad[5] = "notabip39word"
	if _, err := ToSeed(strings.Join(bad, " ")); !errors.Is(err, ErrUnknownWord) {
		t.Fatalf("чужое слово: %v", err)
	}

	// Битая чек-сумма: подменим последнее слово на другое валидное.
	bad = append([]string(nil), words...)
	if bad[23] == "zoo" {
		bad[23] = "zone"
	} else {
		bad[23] = "zoo"
	}
	if _, err := ToSeed(strings.Join(bad, " ")); !errors.Is(err, ErrChecksum) {
		t.Fatalf("чек-сумма: %v", err)
	}
}

func TestValid(t *testing.T) {
	if !Valid(FromSeed([32]byte{9})) {
		t.Fatal("валидная фраза не принята")
	}
	if Valid("abandon abandon abandon") {
		t.Fatal("короткая принята")
	}
}

// FuzzToSeed: фразу вводит человек — произвольный ввод не паникует,
// успешный разбор переживает round-trip.
func FuzzToSeed(f *testing.F) {
	f.Add("")
	f.Add(FromSeed([32]byte{1}))
	f.Add(strings.Repeat("abandon ", 23) + "art")
	f.Add("ZOO zoo zoo")

	f.Fuzz(func(t *testing.T, phrase string) {
		seed, err := ToSeed(phrase)
		if err != nil {
			return
		}
		if FromSeed(seed) == "" {
			t.Fatal("кодирование пустое")
		}
		seed2, err := ToSeed(FromSeed(seed))
		if err != nil || seed2 != seed {
			t.Fatalf("round-trip: %v", err)
		}
	})
}
