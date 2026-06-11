package ipc

import "sync/atomic"

// Stats — счётчики IPC-сервера.
type Stats struct {
	AuthFailures  uint64 // подключения с неверным токеном
	Malformed     uint64 // нечитаемые кадры от UI
	EventsDropped uint64 // события без клиента или при полной очереди
}

type counters struct {
	authFailures  atomic.Uint64
	malformed     atomic.Uint64
	eventsDropped atomic.Uint64
}

// Stats — снапшот счётчиков.
func (s *Server) Stats() Stats {
	return Stats{
		AuthFailures:  s.ctr.authFailures.Load(),
		Malformed:     s.ctr.malformed.Load(),
		EventsDropped: s.ctr.eventsDropped.Load(),
	}
}
