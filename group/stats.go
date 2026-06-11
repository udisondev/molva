package group

import "sync/atomic"

// Stats — счётчики движка групп.
type Stats struct {
	Welcomes       uint64 // принятые приглашения
	KeysReceived   uint64 // принятые раздачи sender keys
	Rekeys         uint64 // собственные rekey
	SealedSent     uint64 // отправленные отложенные рассылки
	SealedFailures uint64 // ошибки рассылок (кроме «нет сессии»)
	KeyMisses      uint64 // сообщения раньше ключа (доставка переиграется)
	Undecryptable  uint64 // съеденные нечитаемые (подпись/повтор/дыра)
	Malformed      uint64 // нечитаемые групповые payload'ы
	Refused        uint64 // чужие группы/не участники/понижения версий
	StoreFailures  uint64 // ошибки store в фоне
}

type counters struct {
	welcomes       atomic.Uint64
	keysReceived   atomic.Uint64
	rekeys         atomic.Uint64
	sealedSent     atomic.Uint64
	sealedFailures atomic.Uint64
	keyMisses      atomic.Uint64
	undecryptable  atomic.Uint64
	malformed      atomic.Uint64
	refused        atomic.Uint64
	storeFailures  atomic.Uint64
}

// Stats — снапшот счётчиков.
func (m *Manager) Stats() Stats {
	return Stats{
		Welcomes:       m.ctr.welcomes.Load(),
		KeysReceived:   m.ctr.keysReceived.Load(),
		Rekeys:         m.ctr.rekeys.Load(),
		SealedSent:     m.ctr.sealedSent.Load(),
		SealedFailures: m.ctr.sealedFailures.Load(),
		KeyMisses:      m.ctr.keyMisses.Load(),
		Undecryptable:  m.ctr.undecryptable.Load(),
		Malformed:      m.ctr.malformed.Load(),
		Refused:        m.ctr.refused.Load(),
		StoreFailures:  m.ctr.storeFailures.Load(),
	}
}
