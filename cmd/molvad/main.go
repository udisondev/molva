// molvad — ядро molva: nodenet-узел, протокол сообщений, криптография и
// хранилище. Запускается Electron-оболочкой как дочерний процесс; IPC —
// WebSocket на 127.0.0.1, порт и одноразовый auth-токен приходят через
// окружение (MOLVA_IPC_PORT, MOLVA_IPC_TOKEN).
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

func main() {
	dataDir := flag.String("data", defaultDataDir(), "каталог данных (seed, база)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	if err := run(log, *dataDir); err != nil {
		log.Error("molvad завершился с ошибкой", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, dataDir string) error {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return fmt.Errorf("каталог данных: %w", err)
	}
	log.Info("molvad запускается", "data", dataDir)
	// Подсистемы (узел, store, IPC) подключаются по мере готовности пакетов.
	return nil
}

func defaultDataDir() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		return ".molva"
	}
	return filepath.Join(dir, "molva")
}
