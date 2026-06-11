// Package mnemonic кодирует master-seed molva в человекочитаемую фразу из
// 24 слов и обратно — единственный секрет для бэкапа (ADR §7) пользователь
// записывает словами, а не сырыми байтами. Схема BIP39: 256-битная
// энтропия (наш seed) + 8 бит чек-суммы SHA-256 = 264 бита = 24×11.
// Словарь — официальный BIP39 (английский), вшит через go:embed.
package mnemonic

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	_ "embed"
	"errors"
	"strings"
	"sync"
)

//go:embed english.txt
var wordlistRaw []byte

const (
	// Words — длина фразы для 32-байтового seed.
	Words = 24
	// seedBits/checksumBits — размеры по BIP39 для 256-битной энтропии.
	seedBits     = 256
	checksumBits = seedBits / 32 // 8
)

// Ошибки разбора недоверенной фразы (вводит пользователь — рукой/вставкой).
var (
	ErrWordCount   = errors.New("mnemonic: фраза должна быть из 24 слов")
	ErrUnknownWord = errors.New("mnemonic: слово вне словаря BIP39")
	ErrChecksum    = errors.New("mnemonic: фраза повреждена (чек-сумма)")
)

var (
	wordsOnce sync.Once
	wordlist  [2048]string
	wordIndex map[string]int
)

func loadWordlist() {
	wordIndex = make(map[string]int, 2048)
	sc := bufio.NewScanner(bytes.NewReader(wordlistRaw))
	i := 0
	for sc.Scan() {
		w := strings.TrimSpace(sc.Text())
		if w == "" {
			continue
		}
		if i >= 2048 {
			panic("mnemonic: словарь длиннее 2048 слов")
		}
		wordlist[i] = w
		wordIndex[w] = i
		i++
	}
	if i != 2048 {
		panic("mnemonic: словарь не равен 2048 словам")
	}
}

// FromSeed кодирует seed в 24 слова, разделённые пробелом.
func FromSeed(seed [32]byte) string {
	wordsOnce.Do(loadWordlist)

	sum := sha256.Sum256(seed[:])
	// Биты: seed (256) || старшие checksumBits байта чек-суммы.
	bits := make([]byte, 0, seedBits+checksumBits)
	for _, b := range seed {
		bits = appendBits(bits, b, 8)
	}
	bits = appendBits(bits, sum[0], checksumBits)

	var sb strings.Builder
	for i := range Words {
		idx := 0
		for j := range 11 {
			idx = idx<<1 | int(bits[i*11+j])
		}
		if i > 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(wordlist[idx])
	}
	return sb.String()
}

// ToSeed разбирает фразу обратно в seed, проверяя чек-сумму. Лишние
// пробелы и регистр нормализуются; неизвестное слово или битая чек-сумма —
// ошибка.
func ToSeed(phrase string) ([32]byte, error) {
	wordsOnce.Do(loadWordlist)

	words := strings.Fields(strings.ToLower(strings.TrimSpace(phrase)))
	if len(words) != Words {
		return [32]byte{}, ErrWordCount
	}
	bits := make([]byte, 0, seedBits+checksumBits)
	for _, w := range words {
		idx, ok := wordIndex[w]
		if !ok {
			return [32]byte{}, ErrUnknownWord
		}
		bits = appendBits(bits, byte(idx>>8), 3)
		bits = appendBits(bits, byte(idx), 8)
	}

	var seed [32]byte
	for i := range seed {
		var b byte
		for j := range 8 {
			b = b<<1 | bits[i*8+j]
		}
		seed[i] = b
	}
	// Сверить чек-сумму.
	sum := sha256.Sum256(seed[:])
	for j := range checksumBits {
		if bits[seedBits+j] != (sum[0]>>(7-j))&1 {
			return [32]byte{}, ErrChecksum
		}
	}
	return seed, nil
}

// Valid сообщает, корректна ли фраза (для подсветки ввода в UI).
func Valid(phrase string) bool {
	_, err := ToSeed(phrase)
	return err == nil
}

// appendBits добавляет младшие n бит b (старший — первым).
func appendBits(dst []byte, b byte, n int) []byte {
	for i := n - 1; i >= 0; i-- {
		dst = append(dst, (b>>i)&1)
	}
	return dst
}
