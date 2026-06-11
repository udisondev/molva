// Package apptest — тестовый harness molva: кластер из N ядер на in-memory
// транспорте nodenet (mem.Hub) под testing/synctest. Всё детерминировано:
// фейковые часы, никакого реального I/O сети. Используется e2e-тестами
// каждого шага разработки.
//
// Всегда вызывайте NewCluster внутри synctest.Test.
package apptest

import (
	"encoding/binary"
	"time"

	"github.com/udisondev/nodenet/identity"
	"github.com/udisondev/nodenet/node"
)

// SeedFor превращает маленькое число в детерминированный master-seed —
// тестам нужны стабильные различимые идентичности.
func SeedFor(i uint64) [identity.SeedLen]byte {
	var s [identity.SeedLen]byte
	binary.BigEndian.PutUint64(s[:], i)
	return s
}

// shortMaintenance — интервалы самоподдержки, ужатые под фейковые часы:
// топология сходится за секунды виртуального времени.
func shortMaintenance() node.Maintenance {
	return node.Maintenance{
		Tick:             1 * time.Second,
		KeepaliveSibling: 2 * time.Second,
		KeepaliveFinger:  3 * time.Second,
		DeadSibling:      6 * time.Second,
		DeadFinger:       9 * time.Second,
		SelfLookup:       3 * time.Second,
		SiblingExchange:  3 * time.Second,
		DialTimeout:      2 * time.Second,
		BackoffBase:      1 * time.Second,
		BackoffMax:       60 * time.Second,
		Dialers:          4,
	}
}
