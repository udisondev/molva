// Package blob — передача файлов 1:1: манифест через ratchet-сессию,
// чанки по прямому ребру с шифрованием одноразовым файловым ключом,
// оконный pull получателя, резюм по битмапу.
package blob

import (
	"errors"
	"fmt"

	"github.com/udisondev/molva/proto/blobpb"
	"google.golang.org/protobuf/proto"
)

const (
	// ChunkSize — размер чанка: запас от кадра nodenet под заголовки.
	ChunkSize = 60 << 10
	// Window — окно pull-запроса.
	Window = 64
	// MaxFileSize — потолок файла v1 (битмапы и nonce-схема рассчитаны
	// с запасом, потолок отсекает абсурдные манифесты).
	MaxFileSize = 4 << 30
	// maxNameLen — потолок имени файла в манифесте.
	maxNameLen = 255
)

// Ошибки валидации недоверенного входа.
var (
	ErrMalformed = errors.New("blob: не разбирается")
	ErrBadChunk  = errors.New("blob: чанк не проходит проверку")
)

// Manifest — разобранный манифест файла.
type Manifest struct {
	FileID    [16]byte
	Name      string
	Mime      string
	Size      uint64
	ChunkSize uint32
	FileKey   [32]byte
	WholeHash [32]byte
}

// Chunks — число чанков файла.
func (m *Manifest) Chunks() int {
	return int((m.Size + uint64(m.ChunkSize) - 1) / uint64(m.ChunkSize))
}

// EncodeManifest сериализует манифест (он поедет внутрь ratchet-шифртекста).
func EncodeManifest(m Manifest) ([]byte, error) {
	if err := validateManifest(&m); err != nil {
		return nil, err
	}
	return proto.Marshal(&blobpb.Manifest{
		FileId:    m.FileID[:],
		Name:      m.Name,
		Mime:      m.Mime,
		Size:      m.Size,
		ChunkSize: m.ChunkSize,
		FileKey:   m.FileKey[:],
		WholeHash: m.WholeHash[:],
	})
}

// DecodeManifest разбирает недоверенный манифест.
func DecodeManifest(b []byte) (Manifest, error) {
	var pb blobpb.Manifest
	if err := proto.Unmarshal(b, &pb); err != nil {
		return Manifest{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if len(pb.FileId) != 16 || len(pb.FileKey) != 32 || len(pb.WholeHash) != 32 {
		return Manifest{}, ErrMalformed
	}
	m := Manifest{
		Name:      pb.Name,
		Mime:      pb.Mime,
		Size:      pb.Size,
		ChunkSize: pb.ChunkSize,
	}
	copy(m.FileID[:], pb.FileId)
	copy(m.FileKey[:], pb.FileKey)
	copy(m.WholeHash[:], pb.WholeHash)
	if err := validateManifest(&m); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

func validateManifest(m *Manifest) error {
	if m.Size == 0 || m.Size > MaxFileSize {
		return fmt.Errorf("%w: размер %d", ErrMalformed, m.Size)
	}
	if m.ChunkSize == 0 || m.ChunkSize > ChunkSize {
		return fmt.Errorf("%w: чанк %d", ErrMalformed, m.ChunkSize)
	}
	if m.Name == "" || len(m.Name) > maxNameLen {
		return fmt.Errorf("%w: имя", ErrMalformed)
	}
	if len(m.Mime) > 127 {
		return fmt.Errorf("%w: mime", ErrMalformed)
	}
	return nil
}

// Request — оконный запрос чанков.
type Request struct {
	FileID  [16]byte
	Indexes []uint32
}

// EncodeRequest сериализует запрос.
func EncodeRequest(r Request) ([]byte, error) {
	if len(r.Indexes) == 0 || len(r.Indexes) > Window {
		return nil, fmt.Errorf("%w: окно %d", ErrMalformed, len(r.Indexes))
	}
	return proto.Marshal(&blobpb.ChunkRequest{FileId: r.FileID[:], Indexes: r.Indexes})
}

// DecodeRequest разбирает недоверенный запрос.
func DecodeRequest(b []byte) (Request, error) {
	var pb blobpb.ChunkRequest
	if err := proto.Unmarshal(b, &pb); err != nil {
		return Request{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if len(pb.FileId) != 16 || len(pb.Indexes) == 0 || len(pb.Indexes) > Window {
		return Request{}, ErrMalformed
	}
	var r Request
	copy(r.FileID[:], pb.FileId)
	r.Indexes = pb.Indexes
	return r, nil
}

// Chunk — один чанк с шифрованным содержимым.
type Chunk struct {
	FileID  [16]byte
	Index   uint32
	Payload []byte
}

// EncodeChunk сериализует чанк.
func EncodeChunk(c Chunk) ([]byte, error) {
	if len(c.Payload) == 0 || len(c.Payload) > ChunkSize+64 {
		return nil, fmt.Errorf("%w: payload %d", ErrMalformed, len(c.Payload))
	}
	return proto.Marshal(&blobpb.Chunk{FileId: c.FileID[:], Index: c.Index, Payload: c.Payload})
}

// DecodeChunk разбирает недоверенный чанк.
func DecodeChunk(b []byte) (Chunk, error) {
	var pb blobpb.Chunk
	if err := proto.Unmarshal(b, &pb); err != nil {
		return Chunk{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if len(pb.FileId) != 16 || len(pb.Payload) == 0 || len(pb.Payload) > ChunkSize+64 {
		return Chunk{}, ErrMalformed
	}
	var c Chunk
	copy(c.FileID[:], pb.FileId)
	c.Index = pb.Index
	c.Payload = pb.Payload
	return c, nil
}
