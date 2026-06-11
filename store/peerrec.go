package store

import "github.com/udisondev/molva/peer"

// PeerState — состояние пира в круге общения. Одобрения знакомства нет:
// пир либо в эфире (добавлен по инвайту или написал первым), либо в чёрном
// списке. Блок терминален до Unblock; разблокировка возвращает в незнакомцы
// (запись удаляется). Числовые значения зашиты в БД — менять нельзя
// (1 и 2 занимали упразднённые pending-состояния).
type PeerState uint8

const (
	// PeerNone — записи нет: незнакомец.
	PeerNone PeerState = 0
	// PeerContact — пир в эфире: переписка и звонки без одобрения.
	PeerContact PeerState = 3
	// PeerBlocked — заблокирован: весь трафик дропается без ack.
	PeerBlocked PeerState = 4
)

// PeerInfo — запись о пире с расшифрованным алиасом.
type PeerInfo struct {
	Peer      peer.ID
	State     PeerState
	Alias     string
	CreatedAt int64
	UpdatedAt int64
}
