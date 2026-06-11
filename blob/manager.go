package blob

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/udisondev/molva/chat"
	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/store"
	"golang.org/x/crypto/blake2b"
)

const (
	// windowTimeout — молчание окна до повторного запроса недостающих.
	windowTimeout = 8 * time.Second
	// pullTick — период обхода активных приёмов.
	pullTick = 5 * time.Second
	// dirtyFlush — каждые столько принятых чанков битмап уезжает в store.
	dirtyFlush = 64
	// sendTimeout — потолок отправки одного чанка/запроса.
	sendTimeout = 5 * time.Second
)

// SendFunc отдаёт кадр пиру (вкалывается из app).
type SendFunc func(ctx context.Context, to peer.ID, frame []byte) error

// inFrame — входящий fast-конверт, переданный с цикла доставки ядра.
type inFrame struct {
	from peer.ID
	env  envelope.Envelope
}

// pull — активный приём файла.
type pull struct {
	peer   peer.ID
	man    Manifest
	bm     *Bitmap
	f      *os.File
	window []uint32
	reqAt  time.Time
	dirty  int
}

// Manager — движок передачи файлов. Приём и тики — одна горутина (без
// гонок на состоянии приёмов), отдача чанков — отдельная. Очереди
// bounded, дропы видны; потерянное переигрывается повтором окна.
type Manager struct {
	db        *store.DB
	chats     *chat.Manager
	dir       string
	rnd       io.Reader
	sendChunk SendFunc
	sendReq   SendFunc
	online    func(peer.ID) bool

	inChunks chan inFrame
	inReqs   chan inFrame
	newPulls chan store.FileRec

	pulls map[[16]byte]*pull // трогает только горутина приёма

	onOffered  func(from peer.ID, man Manifest)
	onProgress func(fileID [16]byte, have, total int)
	onDone     func(fileID [16]byte, path string)

	ctr counters
}

// NewManager собирает движок и вешает обработчик манифестов на ratchet-слой.
// dir — каталог входящих файлов (создаётся).
func NewManager(db *store.DB, chats *chat.Manager, dir string, sendChunk, sendReq SendFunc, online func(peer.ID) bool) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("blob: каталог файлов: %w", err)
	}
	m := &Manager{
		db:        db,
		chats:     chats,
		dir:       dir,
		rnd:       rand.Reader,
		sendChunk: sendChunk,
		sendReq:   sendReq,
		online:    online,
		inChunks:  make(chan inFrame, 256),
		inReqs:    make(chan inFrame, 64),
		newPulls:  make(chan store.FileRec, 16),
		pulls:     make(map[[16]byte]*pull),
	}
	chats.RegisterSealed(envelope.TypeFileManifest, m.onManifest)
	return m, nil
}

// SetCallbacks — события для слоя представления (до запуска ядра).
func (m *Manager) SetCallbacks(onOffered func(peer.ID, Manifest), onProgress func([16]byte, int, int), onDone func([16]byte, string)) {
	m.onOffered = onOffered
	m.onProgress = onProgress
	m.onDone = onDone
}

// HandleChunk — fast-обработчик TYPE_FILE_CHUNK (с цикла доставки ядра).
func (m *Manager) HandleChunk(from peer.ID, env *envelope.Envelope) {
	select {
	case m.inChunks <- inFrame{from: from, env: *env}:
	default:
		m.ctr.chunksDropped.Add(1) // окно переспросит
	}
}

// HandleChunkReq — fast-обработчик TYPE_FILE_CHUNK_REQ.
func (m *Manager) HandleChunkReq(from peer.ID, env *envelope.Envelope) {
	select {
	case m.inReqs <- inFrame{from: from, env: *env}:
	default:
		m.ctr.reqsDropped.Add(1) // получатель переспросит по таймауту
	}
}

// Offer предлагает файл контакту: манифест уезжает через ratchet-сессию.
// Без готовой сессии — chat.ErrNoSession (рукопожатие уже запущено).
func (m *Manager) Offer(ctx context.Context, to peer.ID, path string) ([16]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return [16]byte{}, fmt.Errorf("blob: файл: %w", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return [16]byte{}, fmt.Errorf("blob: файл: %w", err)
	}
	if st.Size() == 0 || st.Size() > MaxFileSize {
		return [16]byte{}, fmt.Errorf("blob: размер файла %d вне пределов", st.Size())
	}

	h, err := blake2b.New256(nil)
	if err != nil {
		return [16]byte{}, err
	}
	if _, err := io.Copy(h, f); err != nil {
		return [16]byte{}, fmt.Errorf("blob: хэширование: %w", err)
	}

	man := Manifest{
		Name:      filepath.Base(path),
		Mime:      "application/octet-stream",
		Size:      uint64(st.Size()),
		ChunkSize: ChunkSize,
	}
	if _, err := io.ReadFull(m.rnd, man.FileID[:]); err != nil {
		return [16]byte{}, err
	}
	if _, err := io.ReadFull(m.rnd, man.FileKey[:]); err != nil {
		return [16]byte{}, err
	}
	copy(man.WholeHash[:], h.Sum(nil))

	manBytes, err := EncodeManifest(man)
	if err != nil {
		return [16]byte{}, err
	}
	if _, err := m.chats.SendSealed(ctx, to, envelope.TypeFileManifest, manBytes); err != nil {
		return [16]byte{}, err
	}
	now := time.Now().UnixMilli()
	err = m.db.Tx(ctx, func(tx *store.Tx) error {
		return tx.FilePut(&store.FileRec{
			FileID: man.FileID, Peer: to, Outgoing: true, Manifest: manBytes,
			Path: path, Bitmap: nil, Done: true, CreatedAt: now, UpdatedAt: now,
		})
	})
	if err != nil {
		return [16]byte{}, err
	}
	return man.FileID, nil
}

// onManifest — входящий манифест (внутри транзакции приёма ratchet-слоя).
func (m *Manager) onManifest(tx *store.Tx, from peer.ID, plain []byte) error {
	man, err := DecodeManifest(plain)
	if err != nil {
		m.ctr.malformed.Add(1)
		return nil // мусор от аутентифицированного контакта: съесть
	}
	rec := store.FileRec{
		FileID: man.FileID, Peer: from, Outgoing: false, Manifest: plain,
		Path:   filepath.Join(m.dir, fmt.Sprintf("%x.part", man.FileID)),
		Bitmap: NewBitmap(man.Chunks()).Bytes(),
		CreatedAt: time.Now().UnixMilli(), UpdatedAt: time.Now().UnixMilli(),
	}
	tx.AfterCommit(func() {
		m.ctr.manifestsRecv.Add(1)
		if m.onOffered != nil {
			m.onOffered(from, man)
		}
		select {
		case m.newPulls <- rec:
		default:
			m.ctr.pullsDropped.Add(1) // рестарт ядра возобновит по store
		}
	})
	return tx.FilePut(&rec)
}

// Run крутит приём, отдачу и тики до отмены ctx.
func (m *Manager) Run(ctx context.Context) error {
	// Резюм незавершённых приёмов после рестарта.
	if recs, err := m.db.FileListUnfinished(ctx); err == nil {
		for _, rec := range recs {
			m.startPull(ctx, rec)
		}
	}

	go m.serveLoop(ctx)

	ticker := time.NewTicker(pullTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			for _, p := range m.pulls {
				m.persistPull(context.Background(), p, false)
				_ = p.f.Close()
			}
			return ctx.Err()
		case rec := <-m.newPulls:
			m.startPull(ctx, rec)
		case f := <-m.inChunks:
			m.acceptChunk(ctx, f)
		case <-ticker.C:
			now := time.Now()
			for _, p := range m.pulls {
				if now.Sub(p.reqAt) > windowTimeout && m.online(p.peer) {
					m.requestWindow(ctx, p)
				}
			}
		}
	}
}

// serveLoop отвечает на оконные запросы (отдельная горутина: чтение
// файла и отправка чанков не должны тормозить приём).
func (m *Manager) serveLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case f := <-m.inReqs:
			m.serveRequest(ctx, f)
		}
	}
}

func (m *Manager) serveRequest(ctx context.Context, in inFrame) {
	req, err := DecodeRequest(in.env.Payload)
	if err != nil {
		m.ctr.malformed.Add(1)
		return
	}
	rec, ok, err := m.db.FileGet(ctx, req.FileID)
	if err != nil || !ok || !rec.Outgoing || rec.Peer != in.from {
		// Чужой или неизвестный файл: чанки получает только адресат оффера.
		m.ctr.reqsRefused.Add(1)
		return
	}
	man, err := DecodeManifest(rec.Manifest)
	if err != nil {
		m.ctr.malformed.Add(1)
		return
	}
	f, err := os.Open(rec.Path)
	if err != nil {
		m.ctr.serveErrors.Add(1)
		return
	}
	defer f.Close()

	buf := make([]byte, ChunkSize)
	for _, idx := range req.Indexes {
		n := chunkLen(&man, idx)
		if n == 0 {
			m.ctr.reqsRefused.Add(1)
			return
		}
		if _, err := f.ReadAt(buf[:n], int64(idx)*int64(man.ChunkSize)); err != nil {
			m.ctr.serveErrors.Add(1)
			return
		}
		payload := sealChunk(man.FileKey, man.FileID, idx, buf[:n])
		mid, err := envelope.NewMsgID(m.rnd)
		if err != nil {
			return
		}
		chunkBytes, err := EncodeChunk(Chunk{FileID: man.FileID, Index: idx, Payload: payload})
		if err != nil {
			return
		}
		frame, err := envelope.Encode(envelope.Envelope{
			MsgID: mid, Type: envelope.TypeFileChunk, Payload: chunkBytes,
		})
		if err != nil {
			return
		}
		sctx, cancel := context.WithTimeout(ctx, sendTimeout)
		err = m.sendChunk(sctx, in.from, frame)
		cancel()
		if err != nil {
			// Ребро умерло — бросаем окно, получатель переспросит.
			m.ctr.serveErrors.Add(1)
			return
		}
		m.ctr.chunksSent.Add(1)
	}
	m.ctr.reqsServed.Add(1)
}

// startPull открывает (или продолжает) приём файла.
func (m *Manager) startPull(ctx context.Context, rec store.FileRec) {
	if _, exists := m.pulls[rec.FileID]; exists {
		return
	}
	man, err := DecodeManifest(rec.Manifest)
	if err != nil {
		m.ctr.malformed.Add(1)
		return
	}
	f, err := os.OpenFile(rec.Path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		m.ctr.pullErrors.Add(1)
		return
	}
	p := &pull{
		peer: rec.Peer,
		man:  man,
		bm:   BitmapFromBytes(rec.Bitmap, man.Chunks()),
		f:    f,
	}
	m.pulls[man.FileID] = p
	m.requestWindow(ctx, p)
}

// requestWindow шлёт запрос очередного окна недостающих чанков.
func (m *Manager) requestWindow(ctx context.Context, p *pull) {
	missing := p.bm.Missing(Window)
	if len(missing) == 0 {
		return
	}
	reqBytes, err := EncodeRequest(Request{FileID: p.man.FileID, Indexes: missing})
	if err != nil {
		return
	}
	mid, err := envelope.NewMsgID(m.rnd)
	if err != nil {
		return
	}
	frame, err := envelope.Encode(envelope.Envelope{
		MsgID: mid, Type: envelope.TypeFileChunkReq, Payload: reqBytes,
	})
	if err != nil {
		return
	}
	p.window = missing
	p.reqAt = time.Now()
	sctx, cancel := context.WithTimeout(ctx, sendTimeout)
	_ = m.sendReq(sctx, p.peer, frame) // молчание лечится тиком
	cancel()
}

// acceptChunk проверяет, расшифровывает и пишет принятый чанк.
func (m *Manager) acceptChunk(ctx context.Context, in inFrame) {
	c, err := DecodeChunk(in.env.Payload)
	if err != nil {
		m.ctr.malformed.Add(1)
		return
	}
	p, ok := m.pulls[c.FileID]
	if !ok || in.from != p.peer {
		m.ctr.lateChunks.Add(1)
		return
	}
	want := chunkLen(&p.man, c.Index)
	if want == 0 || p.bm.Has(int(c.Index)) {
		m.ctr.lateChunks.Add(1)
		return
	}
	plain, err := openChunk(p.man.FileKey, p.man.FileID, c.Index, c.Payload)
	if err != nil || len(plain) != want {
		m.ctr.badChunks.Add(1)
		return
	}
	if _, err := p.f.WriteAt(plain, int64(c.Index)*int64(p.man.ChunkSize)); err != nil {
		m.ctr.pullErrors.Add(1)
		return
	}
	p.bm.Set(int(c.Index))
	p.dirty++
	m.ctr.chunksRecv.Add(1)
	if m.onProgress != nil {
		m.onProgress(p.man.FileID, p.bm.Count(), p.man.Chunks())
	}

	switch {
	case p.bm.Complete():
		m.finishPull(ctx, p)
	default:
		if p.dirty >= dirtyFlush {
			m.persistPull(ctx, p, false)
		}
		if windowDone(p) {
			m.requestWindow(ctx, p)
		}
	}
}

// windowDone — все запрошенные индексы на месте.
func windowDone(p *pull) bool {
	for _, idx := range p.window {
		if !p.bm.Has(int(idx)) {
			return false
		}
	}
	return true
}

// persistPull сохраняет битмап (и done) в store.
func (m *Manager) persistPull(ctx context.Context, p *pull, done bool) {
	p.dirty = 0
	err := m.db.Tx(ctx, func(tx *store.Tx) error {
		return tx.FileProgress(p.man.FileID, p.bm.Bytes(), done, time.Now().UnixMilli())
	})
	if err != nil {
		m.ctr.pullErrors.Add(1)
	}
}

// finishPull сверяет файл целиком и переименовывает из .part.
func (m *Manager) finishPull(ctx context.Context, p *pull) {
	defer delete(m.pulls, p.man.FileID)
	defer p.f.Close()

	h, err := blake2b.New256(nil)
	if err != nil {
		return
	}
	if _, err := p.f.Seek(0, io.SeekStart); err != nil {
		m.ctr.pullErrors.Add(1)
		return
	}
	if _, err := io.Copy(h, p.f); err != nil {
		m.ctr.pullErrors.Add(1)
		return
	}
	if [32]byte(h.Sum(nil)) != p.man.WholeHash {
		// Отправитель прислал не то, что обещал манифестом; перекачивать
		// бессмысленно — тот же источник. Приём гибнет со счётчиком.
		m.ctr.hashMismatch.Add(1)
		m.persistPull(ctx, p, false)
		return
	}

	final := filepath.Join(m.dir, fmt.Sprintf("%x-%s", p.man.FileID[:4], sanitizeName(p.man.Name)))
	partPath := filepath.Join(m.dir, fmt.Sprintf("%x.part", p.man.FileID))
	if err := os.Rename(partPath, final); err != nil {
		m.ctr.pullErrors.Add(1)
		return
	}
	err = m.db.Tx(ctx, func(tx *store.Tx) error {
		if err := tx.FilePath(p.man.FileID, final, time.Now().UnixMilli()); err != nil {
			return err
		}
		return tx.FileProgress(p.man.FileID, p.bm.Bytes(), true, time.Now().UnixMilli())
	})
	if err != nil {
		m.ctr.pullErrors.Add(1)
		return
	}
	m.ctr.filesDone.Add(1)
	if m.onDone != nil {
		m.onDone(p.man.FileID, final)
	}
}

// sanitizeName чистит имя из манифеста: никакого обхода каталогов.
func sanitizeName(name string) string {
	name = filepath.Base(strings.ReplaceAll(name, "\\", "/"))
	name = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, name)
	if name == "" || name == "." || name == ".." {
		return "file"
	}
	return name
}
