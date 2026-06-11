package store

import "github.com/udisondev/molva/peer"

// PeerState — состояние знакомства с пиром. Блок терминален до Unblock;
// разблокировка возвращает в незнакомцы (запись удаляется).
type PeerState uint8

const (
	// PeerNone — записи нет: незнакомец.
	PeerNone PeerState = 0
	// PeerPendingOut — мы отправили запрос знакомства, ждём ответа.
	PeerPendingOut PeerState = 1
	// PeerPendingIn — запрос пришёл нам, ждёт решения пользователя.
	PeerPendingIn PeerState = 2
	// PeerContact — знакомство принято: полноценный контакт.
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
