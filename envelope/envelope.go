// Package envelope — конверт molva: единица обмена между узлами поверх
// nodenet. Кодирование — protobuf (уровень приложения эволюционирует быстро,
// схема важнее аллокаций); валидация — здесь, на каждом недоверенном входе.
package envelope

import (
	"errors"
	"fmt"
	"io"

	"github.com/udisondev/molva/proto/envelopepb"
	"google.golang.org/protobuf/proto"
)

const (
	// MsgIDLen — длина идентификатора сообщения.
	MsgIDLen = 16
	// MaxPayload — потолок payload'а конверта: запас от бюджета кадра
	// nodenet (~65 КиБ минус заголовки маршрутизации) под обвязку protobuf;
	// вмещает файловый чанк 60 КиБ с заголовком.
	MaxPayload = 63 << 10
)

// Ошибки валидации недоверенного входа.
var (
	ErrMalformed   = errors.New("envelope: не разбирается")
	ErrBadMsgID    = errors.New("envelope: msg_id не 16 байт")
	ErrBadType     = errors.New("envelope: неизвестный тип")
	ErrTooLarge    = errors.New("envelope: payload превышает потолок")
)

// MsgID — идентификатор конверта: 16 случайных байт. Ключ ack'а и
// дедупликации (вместе с отправителем).
type MsgID [MsgIDLen]byte

// NewMsgID читает свежий MsgID из r (crypto/rand.Reader в проде).
func NewMsgID(r io.Reader) (MsgID, error) {
	var id MsgID
	if _, err := io.ReadFull(r, id[:]); err != nil {
		return MsgID{}, fmt.Errorf("envelope: msg_id: %w", err)
	}
	return id, nil
}

// Type — тип конверта; значения зеркалят proto-схему.
type Type uint8

const (
	// TypeAck — подтверждение приёма: payload = MsgID подтверждаемого.
	TypeAck Type = 1
	// TypeProbe — presence-зонд: мимо outbox, истории и дедупа.
	TypeProbe Type = 2
	// TypePong — ответ на зонд, отдаётся только взаимным контактам.
	TypePong Type = 3
	// TypeContactRequest — запрос знакомства (открытый payload).
	TypeContactRequest Type = 4
	// TypeContactAccept — принятие знакомства.
	TypeContactAccept Type = 5
	// TypeSessionInit — интерактивная инициализация Double Ratchet.
	TypeSessionInit Type = 6
	// TypeSessionInitAck — ответная половина инициализации.
	TypeSessionInitAck Type = 7
	// TypeChat — личное сообщение: payload — ratchet-сообщение.
	TypeChat Type = 8
	// TypeGroup — групповое сообщение: payload — sender-key ciphertext.
	TypeGroup Type = 9
	// TypeFileManifest — файловый манифест внутри ratchet-сообщения.
	TypeFileManifest Type = 10
	// TypeFileChunkReq — оконный запрос чанков (мимо outbox и дедупа).
	TypeFileChunkReq Type = 11
	// TypeFileChunk — чанк файла, контент зашифрован файловым ключом.
	TypeFileChunk Type = 12
	// TypeGroupWelcome — приглашение в группу (Welcome внутри ratchet).
	TypeGroupWelcome Type = 13
	// TypeGroupUpdate — новая версия членства (Update внутри ratchet).
	TypeGroupUpdate Type = 14
	// TypeGroupKey — раздача sender key (внутри ratchet).
	TypeGroupKey Type = 15

	maxType = TypeGroupKey
)

// Envelope — разобранный конверт. Payload для Decode — копия входа,
// для Encode — алиас на переданный срез.
type Envelope struct {
	MsgID     MsgID
	Type      Type
	FromSeq   uint64
	LamportTS uint64
	Payload   []byte
}

// Encode сериализует конверт, проверяя инварианты отправителя.
func Encode(e Envelope) ([]byte, error) {
	if err := validate(e.Type, len(e.Payload)); err != nil {
		return nil, err
	}
	return proto.Marshal(&envelopepb.Envelope{
		MsgId:     e.MsgID[:],
		Type:      envelopepb.Type(e.Type),
		FromSeq:   e.FromSeq,
		LamportTs: e.LamportTS,
		Payload:   e.Payload,
	})
}

// Decode разбирает недоверенный вход: произвольные байты не должны ни
// паниковать, ни проходить мимо валидации.
func Decode(b []byte) (Envelope, error) {
	var pb envelopepb.Envelope
	if err := proto.Unmarshal(b, &pb); err != nil {
		return Envelope{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if len(pb.MsgId) != MsgIDLen {
		return Envelope{}, ErrBadMsgID
	}
	if pb.Type < 0 || pb.Type > envelopepb.Type(maxType) {
		return Envelope{}, ErrBadType
	}
	if err := validate(Type(pb.Type), len(pb.Payload)); err != nil {
		return Envelope{}, err
	}
	e := Envelope{
		Type:      Type(pb.Type),
		FromSeq:   pb.FromSeq,
		LamportTS: pb.LamportTs,
		Payload:   pb.Payload,
	}
	copy(e.MsgID[:], pb.MsgId)
	return e, nil
}

func validate(t Type, payloadLen int) error {
	if t < TypeAck || t > maxType {
		return ErrBadType
	}
	if payloadLen > MaxPayload {
		return ErrTooLarge
	}
	return nil
}
