package store

import (
	"crypto/hkdf"
	"crypto/sha256"
)

// KeyFromSeed выводит ключ шифрования истории из master-seed. Метка домена
// своя ("molva/..."), чтобы ни один ключ не жил в двух протоколах; бэкап-секрет
// остаётся один — seed.
func KeyFromSeed(seed [32]byte) [32]byte {
	b, err := hkdf.Key(sha256.New, seed[:], nil, "molva/store/v1", 32)
	if err != nil {
		// Параметры статичны и валидны; ошибка возможна только при порче
		// рантайма — это нарушение инварианта, не рабочий случай.
		panic("store: hkdf: " + err.Error())
	}
	var key [32]byte
	copy(key[:], b)
	return key
}
