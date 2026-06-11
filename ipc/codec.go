// Package ipc — мост между ядром molvad и Electron-UI: WebSocket на
// 127.0.0.1, бинарные protobuf-кадры, одноразовый auth-токен из окружения.
// Через IPC ходит расшифрованный контент — поэтому только loopback, токен
// первым кадром и ни байта на других интерфейсах.
package ipc

import (
	"errors"
	"fmt"

	"github.com/udisondev/molva/proto/ipcpb"
	"google.golang.org/protobuf/proto"
)

// MaxFrameLen — потолок одного IPC-кадра (вырастет с медиакадрами).
const MaxFrameLen = 256 << 10

// Ошибки кодека (кадры от renderer'а — недоверенный ввод).
var (
	ErrFrameTooBig = errors.New("ipc: кадр превышает потолок")
	ErrMalformed   = errors.New("ipc: кадр не разбирается")
)

// EncodeFrame сериализует кадр.
func EncodeFrame(f *ipcpb.Frame) ([]byte, error) {
	b, err := proto.Marshal(f)
	if err != nil {
		return nil, fmt.Errorf("ipc: encode: %w", err)
	}
	if len(b) > MaxFrameLen {
		return nil, ErrFrameTooBig
	}
	return b, nil
}

// DecodeFrame разбирает недоверенный кадр: размер, разбор, непустой kind.
func DecodeFrame(b []byte) (*ipcpb.Frame, error) {
	if len(b) > MaxFrameLen {
		return nil, ErrFrameTooBig
	}
	var f ipcpb.Frame
	if err := proto.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if f.Kind == nil {
		return nil, ErrMalformed
	}
	return &f, nil
}
