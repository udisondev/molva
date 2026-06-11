package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/udisondev/nodenet/kad"
	"github.com/udisondev/nodenet/routing"
	"github.com/udisondev/nodenet/transport"
)

// ErrBadBootstrap — строка точки входа не разбирается.
var ErrBadBootstrap = errors.New("app: ожидается hexid@host:port")

func (c *Core) bootstrapFile() string { return filepath.Join(c.dataDir, "bootstrap.txt") }

// BootstrapEntries — текущие точки входа сети (по одной на строку файла).
func (c *Core) BootstrapEntries() []string {
	b, err := os.ReadFile(c.bootstrapFile())
	if err != nil {
		return nil
	}
	var out []string
	for _, l := range strings.Split(string(b), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}

// AddBootstrap добавляет точку входа: проверяет формат, дописывает в файл и
// сразу скармливает узлу (node.Bootstrap безопасен на живом узле — вся
// работа идёт под мьютексами knowledge/edges/probes), так что узел не
// пересоздаётся и соединение UI не рвётся.
func (c *Core) AddBootstrap(entry string) error {
	entry = strings.TrimSpace(entry)
	contact, err := parseBootstrapEntry(entry)
	if err != nil {
		return err
	}
	entries := c.BootstrapEntries()
	for _, e := range entries {
		if e == entry {
			c.node.Bootstrap([]routing.Contact{contact}) // идемпотентно, но освежим
			return nil
		}
	}
	if err := c.writeBootstrap(append(entries, entry)); err != nil {
		return err
	}
	c.node.Bootstrap([]routing.Contact{contact})
	return nil
}

// RemoveBootstrap убирает точку входа из файла. Влияет на следующий старт:
// живой узел уже изучил топологию, удалять её из памяти nodenet не умеет.
func (c *Core) RemoveBootstrap(entry string) error {
	entry = strings.TrimSpace(entry)
	entries := c.BootstrapEntries()
	kept := entries[:0]
	for _, e := range entries {
		if e != entry {
			kept = append(kept, e)
		}
	}
	return c.writeBootstrap(kept)
}

func (c *Core) writeBootstrap(entries []string) error {
	data := ""
	if len(entries) > 0 {
		data = strings.Join(entries, "\n") + "\n"
	}
	if err := os.WriteFile(c.bootstrapFile(), []byte(data), 0o600); err != nil {
		return fmt.Errorf("app: bootstrap файл: %w", err)
	}
	return nil
}

// parseBootstrapEntry разбирает "hexid@host:port" в контакт nodenet.
func parseBootstrapEntry(s string) (routing.Contact, error) {
	idStr, hostPort, ok := strings.Cut(s, "@")
	if !ok || idStr == "" || hostPort == "" {
		return routing.Contact{}, ErrBadBootstrap
	}
	nid, err := kad.ParseID(idStr)
	if err != nil {
		return routing.Contact{}, fmt.Errorf("%w: %w", ErrBadBootstrap, err)
	}
	if !strings.Contains(hostPort, ":") {
		return routing.Contact{}, ErrBadBootstrap
	}
	// PublicAnchor: точка входа — стабильный публично-адресуемый узел; дозваниваемся
	// напрямую и терпеливо, без fail-fast перехода в hole-punch (см. parseBootstrap
	// в cmd/molvad). Иначе стратегия дозвона до bootstrap деградирует.
	return routing.Contact{
		ID:    nid,
		Caps:  routing.PublicAnchor,
		Addrs: []transport.Addr{{Net: "quic", Endpoint: hostPort}},
	}, nil
}
