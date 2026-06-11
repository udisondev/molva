// Package media — мост между медиакадрами IPC и медиасессией nodenet:
// исходящие кадры renderer'а уезжают датаграммами, входящие поднимаются
// с временем приёма (вход delay-based оценщика). Горячий путь — ноль
// аллокаций на кадр; сессии заменяются на лету (make-before-break).
package media

import (
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

// Bridge — мост одного звонка. Attach замещает сессию (переустановка
// пути), Detach закрывает; Send не блокируется (ErrMediaBackpressure —
// сигнал перегруза для лестницы пресетов).
type Bridge struct {
	onFrame  FrameFunc
	onClosed func()

	mu      sync.Mutex
	session transport.MediaSession
	gen     int

	ctr counters
}

// NewBridge — мост с колбэками входящих кадров и смерти пути.
func NewBridge(onFrame FrameFunc, onClosed func()) *Bridge {
	return &Bridge{onFrame: onFrame, onClosed: onClosed}
}

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

// Send отправляет медиакадр датаграммой: копия в pooled-буфер, без
// аллокаций и без блокировки.
func (b *Bridge) Send(ch uint8, payload []byte) error {
	b.mu.Lock()
	s := b.session
	b.mu.Unlock()
	if s == nil {
		b.ctr.txNoSession.Add(1)
		return ErrNoSession
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

// pump поднимает входящие датаграммы, пока сессия жива и актуальна.
func (b *Bridge) pump(s transport.MediaSession, gen int) {
	for {
		select {
		case dg, ok := <-s.Datagrams():
			if !ok {
				b.finish(s, gen)
				return
			}
			b.ctr.rxFrames.Add(1)
			if b.onFrame != nil {
				b.onFrame(dg.Channel, dg.RxTime, dg.Pkt.Bytes())
			}
			dg.Pkt.Release()
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
