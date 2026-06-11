package outbox

import "sync/atomic"

// Stats — монотонные счётчики движка с момента запуска; каждый дроп виден.
type Stats struct {
	SendAttempts     uint64 // попытки отправки из очереди
	SendFailures     uint64 // неудачные попытки (пир офлайн и т.п.)
	AcksSent         uint64 // отправленные подтверждения
	AckSendFailures  uint64 // не сумели отправить ack (пир ретраит)
	AcksUnknown      uint64 // ack на неизвестный msg_id (дубль/подделка)
	Delivered        uint64 // подтверждённые доставки наших конвертов
	InboundMalformed uint64 // нечитаемые входящие конверты
	DedupHits        uint64 // пере-доставки, погашенные окном дедупа
	HandlerErrors    uint64 // откаты обработчиков (доставка переиграется)
	Unhandled        uint64 // надёжные типы без обработчика
	KicksDropped     uint64 // переполнение канала Flush (таймер догонит)
}

// counters — живой блок; снапшот отдаёт Stats.
type counters struct {
	sendAttempts     atomic.Uint64
	sendFailures     atomic.Uint64
	acksSent         atomic.Uint64
	ackSendFailures  atomic.Uint64
	acksUnknown      atomic.Uint64
	delivered        atomic.Uint64
	inboundMalformed atomic.Uint64
	dedupHits        atomic.Uint64
	handlerErrors    atomic.Uint64
	unhandled        atomic.Uint64
	kicksDropped     atomic.Uint64
	dedupInserts     atomic.Uint64 // внутренний ритм подрезки окна
}

// Stats — снапшот счётчиков движка.
func (m *Manager) Stats() Stats {
	return Stats{
		SendAttempts:     m.ctr.sendAttempts.Load(),
		SendFailures:     m.ctr.sendFailures.Load(),
		AcksSent:         m.ctr.acksSent.Load(),
		AckSendFailures:  m.ctr.ackSendFailures.Load(),
		AcksUnknown:      m.ctr.acksUnknown.Load(),
		Delivered:        m.ctr.delivered.Load(),
		InboundMalformed: m.ctr.inboundMalformed.Load(),
		DedupHits:        m.ctr.dedupHits.Load(),
		HandlerErrors:    m.ctr.handlerErrors.Load(),
		Unhandled:        m.ctr.unhandled.Load(),
		KicksDropped:     m.ctr.kicksDropped.Load(),
	}
}
