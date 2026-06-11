package media

import (
	"encoding/binary"
	"sync"
	"time"
)

// Лестница пресетов v1 — намеренно примитивная адаптация: вниз по
// локальному backpressure и растяжению фидбек-периода получателя
// (delay-сигнал), вверх — консервативно по таймеру без деградаций.

// Preset — уровень лестницы.
type Preset uint8

const (
	// PresetAudioOnly — только аудио.
	PresetAudioOnly Preset = 0
	// Preset240/480/720 — ступени видео.
	Preset240 Preset = 1
	Preset480 Preset = 2
	Preset720 Preset = 3
)

const (
	// feedbackPeriod — период фидбека получателя (канал 18).
	feedbackPeriod = 100 * time.Millisecond
	// feedbackLen — кадр фидбека: период по часам получателя (мкс) +
	// принятые за период кадры.
	feedbackLen = 12
	// stretchDown — растяжение периода фидбека, сигналящее буферизацию.
	stretchDown = 1.5
	// upgradeAfter — стабильность до шага вверх.
	upgradeAfter = 10 * time.Second
	// adaptTick — период решений адаптера.
	adaptTick = 100 * time.Millisecond
)

func encodeFeedback(periodMicros int64, received uint32) []byte {
	var b [feedbackLen]byte
	binary.BigEndian.PutUint64(b[:8], uint64(periodMicros))
	binary.BigEndian.PutUint32(b[8:], received)
	return b[:]
}

func decodeFeedback(b []byte) (periodMicros int64, received uint32, ok bool) {
	if len(b) != feedbackLen {
		return 0, 0, false
	}
	return int64(binary.BigEndian.Uint64(b[:8])), binary.BigEndian.Uint32(b[8:]), true
}

// Adapter — лестница пресетов отправителя.
type Adapter struct {
	mu        sync.Mutex
	level     Preset
	max       Preset
	lastDrop  uint64
	stableAt  time.Time
	onPreset  func(Preset)
}

// NewAdapter — лестница с потолком max и колбэком смены пресета.
func NewAdapter(max Preset, onPreset func(Preset)) *Adapter {
	return &Adapter{level: max, max: max, stableAt: time.Now(), onPreset: onPreset}
}

// Level — текущий пресет.
func (a *Adapter) Level() Preset {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.level
}

// ObserveTxDrops — локальный сигнал перегруза: рост дропов tx-ring
// (он срабатывает на RTT раньше сетевой потери).
func (a *Adapter) ObserveTxDrops(total uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if total > a.lastDrop {
		a.lastDrop = total
		a.degradeLocked()
	}
}

// ObserveFeedback — фидбек получателя: растяжение его периода против
// номинала значит, что путь копит буфер.
func (a *Adapter) ObserveFeedback(periodMicros int64, received uint32) {
	_ = received // v1: решает только delay-сигнал
	a.mu.Lock()
	defer a.mu.Unlock()
	if float64(periodMicros) > float64(feedbackPeriod.Microseconds())*stretchDown {
		a.degradeLocked()
	}
}

// Tick — консервативный шаг вверх после периода стабильности.
func (a *Adapter) Tick(now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.level < a.max && now.Sub(a.stableAt) >= upgradeAfter {
		a.level++
		a.stableAt = now
		if a.onPreset != nil {
			a.onPreset(a.level)
		}
	}
}

func (a *Adapter) degradeLocked() {
	a.stableAt = time.Now()
	if a.level == PresetAudioOnly {
		return
	}
	a.level--
	if a.onPreset != nil {
		a.onPreset(a.level)
	}
}
