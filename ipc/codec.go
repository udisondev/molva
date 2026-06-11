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

// MaxFrameLen — потолок одного IPC-кадра.
const MaxFrameLen = 256 << 10

// Теги кадров: первый байт каждого WS-сообщения.
const (
	// TagProto — protobuf Frame (команды/события).
	TagProto = 0x00
	// TagMedia — медиакадр звонка: [тег][канал][8Б rx-микросекунды][payload].
	// Исходящие от UI кадры несут нулевой rx.
	TagMedia = 0x01

	// mediaHeaderLen — тег + канал + rx.
	mediaHeaderLen = 1 + 1 + 8
	// MaxMediaPayload — потолок payload'а медиакадра: видеокадр целиком
	// (дробление на сегменты — забота моста, не IPC).
	MaxMediaPayload = 1 << 20
)

// Ошибки кодека (кадры от renderer'а — недоверенный ввод).
var (
	ErrFrameTooBig = errors.New("ipc: кадр превышает потолок")
	ErrMalformed   = errors.New("ipc: кадр не разбирается")
)

// EncodeFrame сериализует protobuf-кадр с тегом.
func EncodeFrame(f *ipcpb.Frame) ([]byte, error) {
	b, err := proto.Marshal(f)
	if err != nil {
		return nil, fmt.Errorf("ipc: encode: %w", err)
	}
	if len(b)+1 > MaxFrameLen {
		return nil, ErrFrameTooBig
	}
	out := make([]byte, 0, len(b)+1)
	out = append(out, TagProto)
	return append(out, b...), nil
}

// DecodeFrame разбирает недоверенный protobuf-кадр (после тега).
func DecodeFrame(b []byte) (*ipcpb.Frame, error) {
	if len(b) > MaxFrameLen {
		return nil, ErrFrameTooBig
	}
	if len(b) < 1 || b[0] != TagProto {
		return nil, ErrMalformed
	}
	var f ipcpb.Frame
	if err := proto.Unmarshal(b[1:], &f); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if f.Kind == nil {
		return nil, ErrMalformed
	}
	return &f, nil
}

// EncodeMediaFrame собирает медиакадр; buf переиспользуется (горячий путь).
func EncodeMediaFrame(buf []byte, ch uint8, rxMicros int64, payload []byte) ([]byte, error) {
	if len(payload) == 0 || len(payload) > MaxMediaPayload {
		return nil, ErrMalformed
	}
	buf = buf[:0]
	buf = append(buf, TagMedia, ch)
	buf = append(buf,
		byte(rxMicros>>56), byte(rxMicros>>48), byte(rxMicros>>40), byte(rxMicros>>32),
		byte(rxMicros>>24), byte(rxMicros>>16), byte(rxMicros>>8), byte(rxMicros))
	return append(buf, payload...), nil
}

// DecodeMediaFrame разбирает недоверенный медиакадр; payload алиасит вход.
func DecodeMediaFrame(b []byte) (ch uint8, rxMicros int64, payload []byte, err error) {
	if len(b) <= mediaHeaderLen || len(b) > mediaHeaderLen+MaxMediaPayload || b[0] != TagMedia {
		return 0, 0, nil, ErrMalformed
	}
	ch = b[1]
	for i := range 8 {
		rxMicros = rxMicros<<8 | int64(b[2+i])
	}
	return ch, rxMicros, b[mediaHeaderLen:], nil
}
