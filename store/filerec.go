package store

import "github.com/udisondev/molva/peer"

// FileRec — передача файла: манифест и путь шифруются storeKey'ем
// (метаданные файла — контент), битмап принятых чанков — основа резюма.
type FileRec struct {
	FileID    [16]byte
	Peer      peer.ID
	Outgoing  bool
	Manifest  []byte // сериализованный манифест (расшифрованный)
	Path      string // локальный путь: источник или приёмник (.part)
	Bitmap    []byte
	Done      bool
	CreatedAt int64
	UpdatedAt int64
}
