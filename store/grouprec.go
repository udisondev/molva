package store

// GroupRec — группа: подписанный админом документ членства хранится как
// есть (для проверок и пересылки), имя — шифрованно.
type GroupRec struct {
	GroupID    [32]byte
	Name       string
	AdminPub   [32]byte
	Version    uint64
	Membership []byte // подписанный документ (расшифрованный)
	Left       bool   // нас удалили или мы вышли
	CreatedAt  int64
	UpdatedAt  int64
}

// SealedItem — отложенная sealed-рассылка (ждёт DR-сессию с адресатом).
type SealedItem struct {
	ID      int64
	Peer    [32]byte
	EnvType uint8
	Payload []byte
}
