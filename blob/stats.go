package blob

import "sync/atomic"

// Stats — счётчики движка файлов; каждый дроп и отказ виден.
type Stats struct {
	ChunksSent    uint64 // отданные чанки
	ChunksRecv    uint64 // принятые и записанные чанки
	ChunksDropped uint64 // переполнение очереди приёма (окно переспросит)
	ReqsServed    uint64 // полностью отданные окна
	ReqsDropped   uint64 // переполнение очереди запросов
	ReqsRefused   uint64 // запросы чужих/неизвестных файлов
	ServeErrors   uint64 // ошибки чтения/отправки при отдаче
	LateChunks    uint64 // чанки без активного приёма или дубли
	BadChunks     uint64 // не прошедшие AEAD/длину
	Malformed     uint64 // нечитаемые манифесты/запросы/чанки
	HashMismatch  uint64 // файл целиком не сошёлся с манифестом
	PullErrors    uint64 // ошибки I/O и store на приёме
	PullsDropped  uint64 // переполнение очереди новых приёмов
	ManifestsRecv uint64 // принятые манифесты
	FilesDone     uint64 // завершённые приёмы
}

type counters struct {
	chunksSent    atomic.Uint64
	chunksRecv    atomic.Uint64
	chunksDropped atomic.Uint64
	reqsServed    atomic.Uint64
	reqsDropped   atomic.Uint64
	reqsRefused   atomic.Uint64
	serveErrors   atomic.Uint64
	lateChunks    atomic.Uint64
	badChunks     atomic.Uint64
	malformed     atomic.Uint64
	hashMismatch  atomic.Uint64
	pullErrors    atomic.Uint64
	pullsDropped  atomic.Uint64
	manifestsRecv atomic.Uint64
	filesDone     atomic.Uint64
}

// Stats — снапшот счётчиков.
func (m *Manager) Stats() Stats {
	return Stats{
		ChunksSent:    m.ctr.chunksSent.Load(),
		ChunksRecv:    m.ctr.chunksRecv.Load(),
		ChunksDropped: m.ctr.chunksDropped.Load(),
		ReqsServed:    m.ctr.reqsServed.Load(),
		ReqsDropped:   m.ctr.reqsDropped.Load(),
		ReqsRefused:   m.ctr.reqsRefused.Load(),
		ServeErrors:   m.ctr.serveErrors.Load(),
		LateChunks:    m.ctr.lateChunks.Load(),
		BadChunks:     m.ctr.badChunks.Load(),
		Malformed:     m.ctr.malformed.Load(),
		HashMismatch:  m.ctr.hashMismatch.Load(),
		PullErrors:    m.ctr.pullErrors.Load(),
		PullsDropped:  m.ctr.pullsDropped.Load(),
		ManifestsRecv: m.ctr.manifestsRecv.Load(),
		FilesDone:     m.ctr.filesDone.Load(),
	}
}
