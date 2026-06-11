package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/udisondev/molva/mnemonic"
	"github.com/udisondev/nodenet/identity"
	"github.com/udisondev/nodenet/pow"
)

// Подкоманды управления личностью: онбординг в Electron'е вызывает их
// до запуска долгоживущего узла. Все выводят машиночитаемые строки в
// stdout (KEY=value), ошибки — в stderr.

// genIdentity минтит новую личность под PoW сети, пишет seed-файл и
// печатает мнемонику для бэкапа. Отказывается перезаписывать существующую.
func genIdentity(dataDir string, dmin int) error {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return fmt.Errorf("каталог данных: %w", err)
	}
	path := seedPath(dataDir)
	if _, err := os.Stat(path); err == nil {
		return errors.New("личность уже существует — удалите каталог данных, чтобы создать новую")
	}
	idn, err := pow.Solve(context.Background(), rand.Reader, dmin)
	if err != nil {
		return fmt.Errorf("минт: %w", err)
	}
	seed := idn.Seed()
	if err := writeSeed(path, seed); err != nil {
		return err
	}
	fmt.Println("NODEID=" + idn.ID().String())
	fmt.Println("MNEMONIC=" + mnemonic.FromSeed(seed))
	return nil
}

// restoreIdentity восстанавливает личность из мнемоники (читается из stdin,
// чтобы секрет не светился в списке процессов). Проверяет PoW-порог сети.
func restoreIdentity(dataDir string, dmin int) error {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return fmt.Errorf("каталог данных: %w", err)
	}
	path := seedPath(dataDir)
	if _, err := os.Stat(path); err == nil {
		return errors.New("личность уже существует — удалите каталог данных перед восстановлением")
	}
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, 4096))
	if err != nil {
		return fmt.Errorf("чтение мнемоники: %w", err)
	}
	seed, err := mnemonic.ToSeed(string(raw))
	if err != nil {
		return err
	}
	if !pow.Satisfies(identity.IDFromSeed(seed), dmin) {
		return fmt.Errorf("эта личность не проходит PoW-порог сети (dmin=%d)", dmin)
	}
	if err := writeSeed(path, seed); err != nil {
		return err
	}
	fmt.Println("NODEID=" + identity.FromSeed(seed).ID().String())
	return nil
}

// showMnemonic печатает мнемонику существующего seed (экспорт бэкапа).
func showMnemonic(dataDir string) error {
	b, err := os.ReadFile(seedPath(dataDir))
	if err != nil {
		return fmt.Errorf("чтение seed: %w", err)
	}
	if len(b) != identity.SeedLen {
		return fmt.Errorf("seed повреждён: %d байт", len(b))
	}
	var seed [identity.SeedLen]byte
	copy(seed[:], b)
	fmt.Println("MNEMONIC=" + mnemonic.FromSeed(seed))
	return nil
}

func seedPath(dataDir string) string { return filepath.Join(dataDir, "molva.seed") }

func writeSeed(path string, seed [identity.SeedLen]byte) error {
	if err := os.WriteFile(path, seed[:], 0o600); err != nil {
		return fmt.Errorf("сохранение seed: %w", err)
	}
	return nil
}
