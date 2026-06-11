// Package media — мост между медиакадрами IPC и медиасессией nodenet:
// исходящие кадры renderer'а уезжают датаграммами, входящие поднимаются
// с временем приёма (вход delay-based оценщика). Горячий путь — ноль
// аллокаций на кадр; сессии заменяются на лету (make-before-break).
package media

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/udisondev/nodenet/transport"
)

// ErrNoSession — медиасессии сейчас нет (звонок не активен или путь умер).
var ErrNoSession = errors.New("media: нет активной медиасессии")

// FrameFunc получает входящий медиакадр. payload алиасит пул транспорта и
// валиден только до возврата — получатель копирует синхронно.
type FrameFunc func(ch uint8, rx time.Time, payload []byte)

// Каналы медиаплана molva.
const (
	// ChAudio — Opus-кадры датаграммами (потеря = пропуск, PLC).
	ChAudio = 16
	// ChVideo — видеокадры надёжными сообщениями с реассемблером.
	ChVideo = 17
	// ChFeedback — фидбек получателя для адаптации (датаграммы).
	ChFeedback = 18
)

// Bridge — мост одного звонка. Attach замещает сессию (переустановка
// пути), Detach закрывает; Send не блокируется (ErrMediaBackpressure —
// сигнал перегруза для лестницы пресетов).
type Bridge struct {
	onFrame  FrameFunc
	onClosed func()

	mu      sync.Mutex
	session transport.MediaSession
	gen     int

	adapter *Adapter
	frameID uint32
	rxCount uint64 // принятые медиакадры (для фидбека), только горутина pump

	ctr counters
}

// NewBridge — мост с колбэками входящих кадров и смерти пути.
func NewBridge(onFrame FrameFunc, onClosed func()) *Bridge {
	return &Bridge{onFrame: onFrame, onClosed: onClosed}
}

// SetAdapter подключает лестницу пресетов (до первого Attach).
func (b *Bridge) SetAdapter(a *Adapter) { b.adapter = a }

// Attach подключает сессию, замещая прежнюю (make-before-break: старый
// путь закрывается только после готовности нового).
func (b *Bridge) Attach(s transport.MediaSession) {
	b.mu.Lock()
	old := b.session
	b.session = s
	b.gen++
	gen := b.gen
	b.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	go b.pump(s, gen)
}

// Detach закрывает текущую сессию.
func (b *Bridge) Detach() {
	b.mu.Lock()
	old := b.session
	b.session = nil
	b.gen++
	b.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
}

// Active — есть ли живая сессия.
func (b *Bridge) Active() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.session != nil
}

// Send отправляет медиакадр: аудио/фидбек — датаграммой без аллокаций и
// блокировки; видео — надёжными сообщениями с дроблением.
func (b *Bridge) Send(ch uint8, payload []byte) error {
	b.mu.Lock()
	s := b.session
	b.mu.Unlock()
	if s == nil {
		b.ctr.txNoSession.Add(1)
		return ErrNoSession
	}
	if ch == ChVideo {
		return b.sendVideo(s, payload)
	}
	p := transport.GetMedia()
	n := copy(p.Buf()[:cap(p.Buf())], payload)
	p.SetLen(n)
	err := s.SendDatagram(ch, p)
	p.Release()
	if err != nil {
		b.ctr.txErrors.Add(1)
		return err
	}
	b.ctr.txFrames.Add(1)
	return nil
}

// sendVideo дробит кадр и шлёт сегменты надёжными сообщениями (без HOL
// между кадрами: каждый сегмент — свой стрим).
func (b *Bridge) sendVideo(s transport.MediaSession, frame []byte) error {
	b.mu.Lock()
	b.frameID++
	id := b.frameID
	b.mu.Unlock()
	err := segmentVideo(id, frame, func(seg []byte) error {
		p := transport.Get()
		n := copy(p.Buf()[:cap(p.Buf())], seg)
		p.SetLen(n)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := s.SendMessage(ctx, ChVideo, p)
		cancel()
		p.Release()
		return err
	})
	if err != nil {
		b.ctr.txErrors.Add(1)
		return err
	}
	b.ctr.txFrames.Add(1)
	return nil
}

// pump поднимает входящие датаграммы и сообщения, шлёт фидбек получателя
// и крутит адаптер, пока сессия жива и актуальна.
func (b *Bridge) pump(s transport.MediaSession, gen int) {
	reasm := NewReassembler()
	feedback := time.NewTicker(feedbackPeriod)
	defer feedback.Stop()
	adapt := time.NewTicker(adaptTick)
	defer adapt.Stop()
	lastFeedback := time.Now()
	var lastRx uint64

	for {
		select {
		case dg, ok := <-s.Datagrams():
			if !ok {
				b.finish(s, gen)
				return
			}
			switch dg.Channel {
			case ChFeedback:
				// Сигнал адаптации отправителя — мимо UI.
				if period, received, ok := decodeFeedback(dg.Pkt.Bytes()); ok && b.adapter != nil {
					b.adapter.ObserveFeedback(period, received)
				}
			default:
				b.ctr.rxFrames.Add(1)
				b.rxCount++
				if b.onFrame != nil {
					b.onFrame(dg.Channel, dg.RxTime, dg.Pkt.Bytes())
				}
			}
			dg.Pkt.Release()

		case msg, ok := <-s.Messages():
			if !ok {
				b.finish(s, gen)
				return
			}
			if msg.Channel == ChVideo {
				if frame, err := reasm.Push(msg.Pkt.Bytes()); err == nil && frame != nil {
					b.ctr.rxFrames.Add(1)
					b.rxCount++
					if b.onFrame != nil {
						b.onFrame(ChVideo, time.Now(), frame)
					}
				} else if err != nil {
					b.ctr.rxBadSegments.Add(1)
				}
			}
			msg.Pkt.Release()

		case now := <-feedback.C:
			period := now.Sub(lastFeedback).Microseconds()
			received := uint64(0)
			if b.rxCount >= lastRx {
				received = b.rxCount - lastRx
			}
			lastFeedback, lastRx = now, b.rxCount
			fb := encodeFeedback(period, uint32(received))
			p := transport.GetMedia()
			n := copy(p.Buf()[:cap(p.Buf())], fb)
			p.SetLen(n)
			_ = s.SendDatagram(ChFeedback, p) // потерянный фидбек не страшен
			p.Release()

		case now := <-adapt.C:
			if b.adapter != nil {
				b.adapter.ObserveTxDrops(s.Stats().TxDroppedQueue)
				b.adapter.Tick(now)
			}

		case <-s.Closed():
			b.finish(s, gen)
			return
		}
	}
}

// finish сигналит о смерти пути, если сессия всё ещё текущая (замещённые
// пути умирают молча).
func (b *Bridge) finish(s transport.MediaSession, gen int) {
	b.mu.Lock()
	current := b.gen == gen && b.session == s
	if current {
		b.session = nil
	}
	b.mu.Unlock()
	if current && b.onClosed != nil {
		b.onClosed()
	}
}
