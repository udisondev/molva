package media

import "sync/atomic"

// Stats — счётчики медиамоста.
type Stats struct {
	TxFrames    uint64 // отправленные кадры
	TxErrors    uint64 // отказ отправки (backpressure/закрытая сессия)
	TxNoSession uint64 // кадры без активной сессии
	TxBadFrame  uint64 // кадры с недопустимым каналом/размером (мимо nodenet)
	RxFrames      uint64 // принятые кадры
	RxBadSegments uint64 // кривые сегменты видеокадров
}

type counters struct {
	txFrames    atomic.Uint64
	txErrors    atomic.Uint64
	txNoSession atomic.Uint64
	txBadFrame  atomic.Uint64
	rxFrames      atomic.Uint64
	rxBadSegments atomic.Uint64
}

// Stats — снапшот счётчиков.
func (b *Bridge) Stats() Stats {
	return Stats{
		TxFrames:    b.ctr.txFrames.Load(),
		TxErrors:    b.ctr.txErrors.Load(),
		TxNoSession: b.ctr.txNoSession.Load(),
		TxBadFrame:  b.ctr.txBadFrame.Load(),
		RxFrames:      b.ctr.rxFrames.Load(),
		RxBadSegments: b.ctr.rxBadSegments.Load(),
	}
}
