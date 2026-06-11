package store

import (
	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
)

// Status — состояние исходящего сообщения; монотонно растёт.
type Status uint8

const (
	// StatusQueued — лежит в outbox, в сеть ещё не уходило (или пир офлайн).
	StatusQueued Status = 1
	// StatusSent — ушло в сеть хотя бы раз, ack не получен.
	StatusSent Status = 2
	// StatusDelivered — получатель подтвердил ack'ом. Честный сигнал.
	StatusDelivered Status = 3
)

// Message — запись истории. Body шифруется storeKey'ем при записи и
// расшифровывается при чтении; после локального удаления Body == nil и
// Deleted == true, метаданные остаются (по ним работает дедуп и порядок).
type Message struct {
	Peer     peer.ID
	MsgID    envelope.MsgID
	Outgoing bool
	FromSeq  uint64
	Lamport  uint64
	SentAt   int64 // unix-миллисекунды локальных часов записи
	Status   Status
	Deleted  bool
	Body     []byte
}
