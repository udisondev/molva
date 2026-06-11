package store

import (
	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
)

// OutboxItem — элемент персистентной очереди исходящих: закодированный
// кадр конверта плюс состояние ретраев. Frame хранится шифрованным
// (storeKey), наружу отдаётся открытым.
type OutboxItem struct {
	ID       int64
	Peer     peer.ID
	MsgID    envelope.MsgID
	Frame    []byte
	Attempts int
	NextAt   int64 // unix-миллисекунды ближайшей попытки
}
