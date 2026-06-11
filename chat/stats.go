package chat

import "sync/atomic"

// Stats — монотонные счётчики движка диалогов; каждый съеденный без
// эффекта конверт виден.
type Stats struct {
	SessionsInitiated   uint64 // отправленные рукопожатия
	SessionsAccepted    uint64 // принятые чужие рукопожатия
	SessionsEstablished uint64 // завершённые свои рукопожатия
	CollisionsIgnored   uint64 // чужие init, проигравшие коллизию
	StaleAcks           uint64 // ответы на отжившие рукопожатия
	Malformed           uint64 // нечитаемые payload'ы диалоговых конвертов
	NoSession           uint64 // CHAT без сессии (съеден, идёт re-handshake)
	DecryptFailures     uint64 // рассинхрон состояний (съеден, re-handshake)
	PendingDrained      uint64 // тексты, дождавшиеся сессии
}

type counters struct {
	sessionsInitiated   atomic.Uint64
	sessionsAccepted    atomic.Uint64
	sessionsEstablished atomic.Uint64
	collisionsIgnored   atomic.Uint64
	staleAcks           atomic.Uint64
	malformed           atomic.Uint64
	noSession           atomic.Uint64
	decryptFailures     atomic.Uint64
	pendingDrained      atomic.Uint64
}

// Stats — снапшот счётчиков.
func (m *Manager) Stats() Stats {
	return Stats{
		SessionsInitiated:   m.ctr.sessionsInitiated.Load(),
		SessionsAccepted:    m.ctr.sessionsAccepted.Load(),
		SessionsEstablished: m.ctr.sessionsEstablished.Load(),
		CollisionsIgnored:   m.ctr.collisionsIgnored.Load(),
		StaleAcks:           m.ctr.staleAcks.Load(),
		Malformed:           m.ctr.malformed.Load(),
		NoSession:           m.ctr.noSession.Load(),
		DecryptFailures:     m.ctr.decryptFailures.Load(),
		PendingDrained:      m.ctr.pendingDrained.Load(),
	}
}
