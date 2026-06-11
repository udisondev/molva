package callsig

import "sync/atomic"

// Stats — счётчики сигналинга.
type Stats struct {
	ConsentRefused uint64 // медиасессии без активного звонка
	BusyRejects    uint64 // входящие при занятой линии
	Stale          uint64 // ответы/hangup на отжившие звонки
	Malformed      uint64 // нечитаемые payload'ы сигналинга
}

type counters struct {
	consentRefused atomic.Uint64
	busyRejects    atomic.Uint64
	stale          atomic.Uint64
	malformed      atomic.Uint64
}

// Stats — снапшот счётчиков.
func (m *Manager) Stats() Stats {
	return Stats{
		ConsentRefused: m.ctr.consentRefused.Load(),
		BusyRejects:    m.ctr.busyRejects.Load(),
		Stale:          m.ctr.stale.Load(),
		Malformed:      m.ctr.malformed.Load(),
	}
}
