// Package peer — идентификатор пользователя molva. Вынесен в отдельный
// лист-пакет, чтобы протокольные пакеты разделяли один тип, не завися от
// nodenet (конверсия с node.ID — прямая: оба [32]byte).
package peer

import "encoding/hex"

// IDLen — длина идентификатора в байтах.
const IDLen = 32

// ID — постоянный идентификатор пользователя: NodeID nodenet,
// BLAKE2b-256 от Ed25519-ключа. Одновременно сетевой адрес и публичная
// личность; самоаутентифицируем (хэш ключа), глобальных имён поверх нет.
type ID [IDLen]byte

// String — низкорегистровый hex, 64 символа.
func (id ID) String() string { return hex.EncodeToString(id[:]) }

// Short — первые 8 hex-символов для логов.
func (id ID) Short() string { return hex.EncodeToString(id[:4]) }
