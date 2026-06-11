// molvad — ядро molva: nodenet-узел, протокол сообщений, криптография и
// хранилище. Запускается Electron-оболочкой как дочерний процесс; IPC —
// WebSocket на 127.0.0.1. Auth-токен приходит через MOLVA_IPC_TOKEN (hex),
// порт — через MOLVA_IPC_PORT (0 или пусто — эфемерный); фактический адрес
// печатается в stdout строкой MOLVA_IPC_ADDR=<host:port>.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/udisondev/molva/app"
	"github.com/udisondev/molva/ipc"
	"github.com/udisondev/molva/media"
	"github.com/udisondev/molva/peer"
	"github.com/udisondev/nodenet/identity"
	"github.com/udisondev/nodenet/kad"
	"github.com/udisondev/nodenet/pow"
	"github.com/udisondev/nodenet/routing"
	"github.com/udisondev/nodenet/transport"
	quictr "github.com/udisondev/nodenet/transport/quic"
)

func main() {
	var (
		dataDir   = flag.String("data", defaultDataDir(), "каталог данных (seed, база)")
		listen    = flag.String("listen", "0.0.0.0:0", "UDP-адрес узла (QUIC)")
		bootstrap = flag.String("bootstrap", "", "точки входа сети: hexid@host:port через запятую")
		dmin      = flag.Int("dmin", 0, "PoW-сложность сети")
		grace     = flag.Duration("grace", 30*time.Second, "жизнь без UI до самовыключения (0 — вечно)")
		// Короткие подкоманды управления личностью (используются Electron'ом
		// при онбординге; делают своё дело и сразу выходят):
		genSeed     = flag.Bool("gen-seed", false, "минт новой личности → печать MNEMONIC, выход")
		restoreSeed = flag.Bool("restore-seed", false, "восстановить личность из мнемоники (stdin), выход")
		showSeed    = flag.Bool("show-mnemonic", false, "печать мнемоники существующего seed, выход")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	switch {
	case *genSeed:
		if err := genIdentity(*dataDir, *dmin); err != nil {
			log.Error("создание личности", "err", err)
			os.Exit(1)
		}
		return
	case *restoreSeed:
		if err := restoreIdentity(*dataDir, *dmin); err != nil {
			log.Error("восстановление личности", "err", err)
			os.Exit(1)
		}
		return
	case *showSeed:
		if err := showMnemonic(*dataDir); err != nil {
			log.Error("показ мнемоники", "err", err)
			os.Exit(1)
		}
		return
	}

	if err := run(log, *dataDir, *listen, *bootstrap, *dmin, *grace); err != nil {
		log.Error("molvad завершился с ошибкой", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, dataDir, listen, bootstrap string, dmin int, grace time.Duration) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return fmt.Errorf("каталог данных: %w", err)
	}
	seed, err := loadOrCreateSeed(ctx, log, filepath.Join(dataDir, "molva.seed"), dmin)
	if err != nil {
		return err
	}
	id := identity.FromSeed(seed)
	log.Info("личность", "node", id.ID().String())

	if bootstrap == "" {
		// Точки входа сети из файла данных; флаг -bootstrap перекрывает.
		if b, err := os.ReadFile(filepath.Join(dataDir, "bootstrap.txt")); err == nil {
			bootstrap = strings.ReplaceAll(strings.TrimSpace(string(b)), "\n", ",")
		}
	}
	contacts, err := parseBootstrap(bootstrap)
	if err != nil {
		return err
	}
	log.Info("бутстрап", "точек", len(contacts))

	tr, err := quictr.Listen(id, listen)
	if err != nil {
		return fmt.Errorf("транспорт: %w", err)
	}
	defer tr.Close()

	token, err := ipcToken()
	if err != nil {
		return err
	}
	srv := ipc.NewServer(token, grace)

	core, err := app.New(app.Config{
		Seed:               seed,
		DataDir:            dataDir,
		Transport:          tr,
		Bootstrap:          contacts,
		Dmin:               dmin,
		OnMessage:          srv.OnMessage,
		OnDelivered:        srv.OnDelivered,
		OnContactRequest:   srv.OnContactRequest,
		OnContactAccept:    srv.OnContactAccept,
		OnPresence:         srv.OnPresence,
		OnFileOffered:      srv.OnFileOffered,
		OnFileProgress:     srv.OnFileProgress,
		OnFileDone:         srv.OnFileDone,
		OnCallIncoming:     srv.OnCallIncoming,
		OnCallState:        srv.OnCallState,
		OnMediaFrame:       srv.PushMedia,
		OnCallReconnecting: srv.OnCallReconnecting,
		OnPreset:           func(l media.Preset) { srv.OnPreset(uint8(l)) },
	})
	if err != nil {
		return err
	}
	srv.Bind(core.Chats(), core.Contacts(), core.Files(), core.Calls(), core, core.Media().Send, core.ReflexiveAddr, core.Store(), peer.ID(core.ID()))

	port := os.Getenv("MOLVA_IPC_PORT")
	if port == "" {
		port = "0"
	}
	addr, err := srv.Listen("127.0.0.1:" + port)
	if err != nil {
		return err
	}
	// Оболочка ждёт эту строку, чтобы узнать порт.
	fmt.Println("MOLVA_IPC_ADDR=" + addr)
	log.Info("ipc слушает", "addr", addr)

	errc := make(chan error, 2)
	go func() { errc <- core.Run(ctx) }()
	go func() { errc <- srv.Run(ctx) }()
	go logStats(ctx, log, core, srv)

	select {
	case <-srv.Idle():
		log.Info("UI не подключён дольше grace-периода — выключаюсь")
		cancel()
		<-errc
	case err := <-errc:
		cancel()
		if err != nil && ctx.Err() == nil {
			return err
		}
	case <-ctx.Done():
		<-errc
	}
	return nil
}

// logStats периодически выгружает счётчики подсистем в лог: каждый дроп и
// отказ виден, иначе наблюдаемость (методы Stats) ни во что не выводилась.
func logStats(ctx context.Context, log *slog.Logger, core *app.Core, srv *ipc.Server) {
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			log.Info("счётчики",
				"outbox", core.Outbox().Stats(),
				"chat", core.Chats().Stats(),
				"group", core.Groups().Stats(),
				"blob", core.Files().Stats(),
				"call", core.Calls().Stats(),
				"media", core.Media().Stats(),
				"ipc", srv.Stats(),
			)
		}
	}
}

// loadOrCreateSeed читает master-seed или гриндит новый под PoW сети.
func loadOrCreateSeed(ctx context.Context, log *slog.Logger, path string, dmin int) ([identity.SeedLen]byte, error) {
	var seed [identity.SeedLen]byte
	b, err := os.ReadFile(path)
	switch {
	case err == nil:
		if len(b) != identity.SeedLen {
			return seed, fmt.Errorf("seed повреждён: %d байт вместо %d", len(b), identity.SeedLen)
		}
		copy(seed[:], b)
		if !pow.Satisfies(identity.IDFromSeed(seed), dmin) {
			return seed, fmt.Errorf("seed не проходит PoW-порог сети (dmin=%d)", dmin)
		}
		return seed, nil
	case os.IsNotExist(err):
		log.Info("первый запуск: создаю личность", "dmin", dmin)
		idn, err := pow.Solve(ctx, rand.Reader, dmin)
		if err != nil {
			return seed, fmt.Errorf("создание личности: %w", err)
		}
		seed = idn.Seed()
		if err := os.WriteFile(path, seed[:], 0o600); err != nil {
			return seed, fmt.Errorf("сохранение seed: %w", err)
		}
		return seed, nil
	default:
		return seed, fmt.Errorf("чтение seed: %w", err)
	}
}

// ipcToken берёт токен из окружения или генерирует (standalone-режим:
// печатается, чтобы к ядру можно было подключиться вручную).
func ipcToken() ([]byte, error) {
	if env := os.Getenv("MOLVA_IPC_TOKEN"); env != "" {
		token, err := hex.DecodeString(env)
		if err != nil || len(token) < 16 {
			return nil, fmt.Errorf("MOLVA_IPC_TOKEN: ожидается hex не короче 32 символов")
		}
		return token, nil
	}
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return nil, err
	}
	fmt.Println("MOLVA_IPC_TOKEN=" + hex.EncodeToString(token))
	return token, nil
}

// parseBootstrap разбирает "hexid@host:port,..." в контакты nodenet.
func parseBootstrap(s string) ([]routing.Contact, error) {
	if s == "" {
		return nil, nil
	}
	var out []routing.Contact
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		idStr, hostPort, ok := strings.Cut(item, "@")
		if !ok {
			return nil, fmt.Errorf("bootstrap: ожидается hexid@host:port, не %q", item)
		}
		nid, err := kad.ParseID(idStr)
		if err != nil {
			return nil, fmt.Errorf("bootstrap %q: %w", item, err)
		}
		out = append(out, routing.Contact{
			ID:    nid,
			Addrs: []transport.Addr{{Net: "quic", Endpoint: hostPort}},
		})
	}
	return out, nil
}

func defaultDataDir() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		return ".molva"
	}
	return filepath.Join(dir, "molva")
}
