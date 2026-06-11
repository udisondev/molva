// Package outbox — слой надёжности molva поверх best-effort доставки
// nodenet: персистентная очередь исходящих с экспоненциальными ретраями,
// ack'и получателя, дедупликация входящих. Всё состояние живёт в store —
// рестарт не теряет ни очередь, ни окно дедупа.
package outbox

import (
	"context"
	"crypto/rand"
	"sync"
	"time"

	"github.com/udisondev/molva/envelope"
	"github.com/udisondev/molva/peer"
	"github.com/udisondev/molva/store"
)

const (
	// retryBase/retryCap — экспоненциальный backoff ретраев.
	retryBase = 5 * time.Second
	retryCap  = time.Hour
	// attemptTimeout — потолок одной попытки отправки (включая Connect).
	attemptTimeout = 15 * time.Second
	// dueBatch — сколько готовых элементов забирается за проход.
	dueBatch = 64
	// flushWorkers — одновременных пиров в проходе отправщика: мёртвый пир
	// жжёт свой таймаут, не задерживая доставку живым.
	flushWorkers = 4
	// dedupMaxAge/dedupCap — окно дедупликации получателя на пира.
	dedupMaxAge = 30 * 24 * time.Hour
	dedupCap    = 100_000
	// pruneEvery — каждая n-я вставка дедупа подрезает окно пира.
	pruneEvery = 4096
)

// SendFunc отдаёт кадр пиру; семантика best-effort, ошибки не фатальны.
type SendFunc func(ctx context.Context, to peer.ID, frame []byte) error

// Handler обрабатывает свежий надёжный конверт внутри транзакции дедупа:
// эффект сообщения и отметка «видели» коммитятся атомарно. Ошибка
// откатывает всё, ack не уходит — отправитель переиграет доставку.
type Handler func(tx *store.Tx, from peer.ID, env *envelope.Envelope) error

// FastHandler обрабатывает ненадёжный конверт (probe/pong): без дедупа,
// без ack'а, без транзакции — молчание и есть ответ.
type FastHandler func(from peer.ID, env *envelope.Envelope)

// Gate решает, пускать ли конверт типа t от пира from в обработку.
// Дроп — без ack'а: для отправителя неотличим от офлайна.
type Gate func(from peer.ID, t envelope.Type) bool

// Manager — движок надёжности. Вся регистрация (Handle, HandleFast,
// SetOnDelivered, SetGate) — строго до Run.
type Manager struct {
	db          *store.DB
	sendQueued  SendFunc // путь очереди: прямое ребро, при нужде Connect
	sendControl SendFunc // путь ack'ов: не блокирующий dispatch-цикл
	handlers    map[envelope.Type]Handler
	fast        map[envelope.Type]FastHandler
	gate        Gate
	onDelivered func(peer.ID, envelope.MsgID)
	kick        chan peer.ID
	ctr         counters
}

// NewManager собирает движок поверх открытой базы. sendQueued — для
// элементов очереди (может блокироваться на Connect), sendControl — для
// ack'ов (обязан не блокироваться: зовётся с цикла доставки).
func NewManager(db *store.DB, sendQueued, sendControl SendFunc) *Manager {
	return &Manager{
		db:          db,
		sendQueued:  sendQueued,
		sendControl: sendControl,
		handlers:    make(map[envelope.Type]Handler),
		fast:        make(map[envelope.Type]FastHandler),
		kick:        make(chan peer.ID, 64),
	}
}

// Handle регистрирует обработчик надёжного типа (до Run).
func (m *Manager) Handle(t envelope.Type, h Handler) { m.handlers[t] = h }

// HandleFast регистрирует обработчик ненадёжного типа (до Run).
func (m *Manager) HandleFast(t envelope.Type, h FastHandler) { m.fast[t] = h }

// SetOnDelivered — колбэк подтверждённой доставки (до Run).
func (m *Manager) SetOnDelivered(f func(peer.ID, envelope.MsgID)) { m.onDelivered = f }

// SetGate — фильтр входящих по отправителю и типу (до Run): блокировка и
// отсев незнакомцев живут уровнем выше, движок лишь дропает со счётчиком.
func (m *Manager) SetGate(g Gate) { m.gate = g }

// EnqueueTx ставит конверт в персистентную очередь внутри транзакции
// вызывающего — запись истории и постановка в очередь коммитятся вместе.
// После коммита нужен Flush, чтобы не ждать таймера.
func (m *Manager) EnqueueTx(tx *store.Tx, to peer.ID, env envelope.Envelope) error {
	frame, err := envelope.Encode(env)
	if err != nil {
		return err
	}
	return tx.OutboxEnqueue(to, env.MsgID, frame, time.Now().UnixMilli())
}

// Flush будит отправщик по пиру: сброс backoff'а и немедленная попытка.
// Неблокирующий; потерянный пинок не страшен — таймер догонит.
func (m *Manager) Flush(p peer.ID) {
	select {
	case m.kick <- p:
	default:
		m.ctr.kicksDropped.Add(1)
	}
}

// Run крутит ретраи до отмены ctx.
func (m *Manager) Run(ctx context.Context) error {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		healthy := m.flushDue(ctx)

		var wake <-chan time.Time
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if !healthy {
			// Ошибка store: холодный повтор вместо busy-spin'а на 100% CPU.
			timer.Reset(retryBase)
			wake = timer.C
		} else if at, ok, err := m.db.OutboxNearest(ctx); err == nil && ok {
			timer.Reset(max(time.Until(time.UnixMilli(at)), 0))
			wake = timer.C
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case p := <-m.kick:
			now := time.Now().UnixMilli()
			err := m.db.Tx(ctx, func(tx *store.Tx) error { return tx.OutboxKick(p, now) })
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				m.ctr.storeFailures.Add(1)
			}
		case <-wake:
		}
	}
}

// attemptResult — итог обращения с одним готовым элементом за проход.
type attemptResult struct {
	item store.OutboxItem
	sent bool // кадр ушёл в сеть
	took bool // попытка была (skipped-элементы не считают attempts)
}

// flushDue — один проход по готовым элементам: группировка по пирам,
// ограниченный пул воркеров (пиры не блокируют друг друга), внутри пира —
// последовательно, первая неудача пира скипает остаток его пачки (один
// Connect-таймаут на пира за проход). Возвращает false при ошибке store.
func (m *Manager) flushDue(ctx context.Context) bool {
	for {
		now := time.Now().UnixMilli()
		due, corrupt, err := m.db.OutboxDue(ctx, now, dueBatch)
		if err != nil {
			m.ctr.storeFailures.Add(1)
			return false
		}
		if len(corrupt) > 0 {
			// Нерасшифровываемые ряды карантинятся удалением: повторное
			// чтение их не вылечит, а очередь они не должны держать.
			err := m.db.Tx(ctx, func(tx *store.Tx) error {
				for _, id := range corrupt {
					if err := tx.OutboxDeleteByID(id); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				m.ctr.storeFailures.Add(1)
				return false
			}
			m.ctr.outboxCorrupt.Add(uint64(len(corrupt)))
		}
		if len(due) == 0 {
			return true
		}

		// Порядок группировки стабилен: пиры в порядке первого появления.
		groups := make(map[peer.ID][]store.OutboxItem)
		var order []peer.ID
		for _, it := range due {
			if _, ok := groups[it.Peer]; !ok {
				order = append(order, it.Peer)
			}
			groups[it.Peer] = append(groups[it.Peer], it)
		}

		results := make([]attemptResult, 0, len(due))
		var mu sync.Mutex
		sem := make(chan struct{}, flushWorkers)
		var wg sync.WaitGroup
		for _, p := range order {
			items := groups[p]
			wg.Go(func() {
				sem <- struct{}{}
				defer func() { <-sem }()
				failed := false
				for _, it := range items {
					if ctx.Err() != nil {
						return
					}
					r := attemptResult{item: it}
					if failed {
						// Пир не отвечает — не жечь таймаут на каждом
						// элементе, остаток пачки только сдвигается.
						mu.Lock()
						results = append(results, r)
						mu.Unlock()
						continue
					}
					actx, cancel := context.WithTimeout(ctx, attemptTimeout)
					sendErr := m.sendQueued(actx, it.Peer, it.Frame)
					cancel()
					m.ctr.sendAttempts.Add(1)
					r.took = true
					if sendErr != nil {
						m.ctr.sendFailures.Add(1)
						failed = true
					} else {
						r.sent = true
					}
					mu.Lock()
					results = append(results, r)
					mu.Unlock()
				}
			})
		}
		wg.Wait()

		// Все переезды next_at — одной транзакцией после прохода.
		now = time.Now().UnixMilli()
		err = m.db.Tx(ctx, func(tx *store.Tx) error {
			for _, r := range results {
				attempts := r.item.Attempts
				if r.took {
					attempts++
				}
				nextAt := now + backoff(max(attempts, 1)).Milliseconds()
				if err := tx.OutboxAttempt(r.item.ID, attempts, nextAt); err != nil {
					return err
				}
				if r.sent {
					// Кадр ушёл в сеть — статус честно «sent» (не доставлен).
					if err := tx.MessageStatusUp(r.item.Peer, r.item.MsgID, store.StatusSent); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			if ctx.Err() != nil {
				return true
			}
			m.ctr.storeFailures.Add(1)
			return false
		}
		if len(due) < dueBatch {
			return true
		}
	}
}

// HandleInbound разбирает входящий payload nodenet и ведёт его по пути
// типа: ack — расчёт очереди, fast — мимо всего, надёжный — дедуп+ack.
// Зовётся с цикла доставки ядра; блокирующих отправок здесь нет.
func (m *Manager) HandleInbound(ctx context.Context, from peer.ID, frame []byte) {
	env, err := envelope.Decode(frame)
	if err != nil {
		m.ctr.inboundMalformed.Add(1)
		return
	}
	if m.gate != nil && !m.gate(from, env.Type) {
		m.ctr.gateDropped.Add(1)
		return
	}
	switch {
	case env.Type == envelope.TypeAck:
		m.handleAck(ctx, from, env)
	case m.fast[env.Type] != nil:
		m.fast[env.Type](from, &env)
	default:
		m.handleReliable(ctx, from, env)
	}
}

func (m *Manager) handleAck(ctx context.Context, from peer.ID, env envelope.Envelope) {
	if len(env.Payload) != envelope.MsgIDLen {
		m.ctr.inboundMalformed.Add(1)
		return
	}
	var orig envelope.MsgID
	copy(orig[:], env.Payload)

	var settled bool
	err := m.db.Tx(ctx, func(tx *store.Tx) error {
		var err error
		settled, err = tx.OutboxSettle(from, orig)
		if err != nil || !settled {
			return err
		}
		return tx.MessageStatusUp(from, orig, store.StatusDelivered)
	})
	if err != nil {
		// Строка осталась в очереди: отправитель ретраит, пир пере-ack'нет.
		if ctx.Err() == nil {
			m.ctr.storeFailures.Add(1)
		}
		return
	}
	if !settled {
		// Ack на неизвестный msg_id: дубль ack'а или подделка — игнор.
		m.ctr.acksUnknown.Add(1)
		return
	}
	m.ctr.delivered.Add(1)
	if m.onDelivered != nil {
		m.onDelivered(from, orig)
	}
}

func (m *Manager) handleReliable(ctx context.Context, from peer.ID, env envelope.Envelope) {
	h := m.handlers[env.Type]
	if h == nil {
		m.ctr.unhandled.Add(1)
		return
	}
	now := time.Now().UnixMilli()
	var fresh bool
	err := m.db.Tx(ctx, func(tx *store.Tx) error {
		var err error
		fresh, err = tx.DedupInsert(from, env.MsgID, now)
		if err != nil || !fresh {
			return err
		}
		if m.ctr.dedupInserts.Add(1)%pruneEvery == 0 {
			if err := tx.DedupPrune(from, now-dedupMaxAge.Milliseconds(), dedupCap); err != nil {
				return err
			}
		}
		return h(tx, from, &env)
	})
	if err != nil {
		// Откатилось целиком (и дедуп тоже) — ретрай отправителя переиграет.
		m.ctr.handlerErrors.Add(1)
		return
	}
	if !fresh {
		m.ctr.dedupHits.Add(1)
	}
	// Ack и на дубль: наш прошлый ack мог не дойти.
	m.sendAck(ctx, from, env.MsgID)
}

func (m *Manager) sendAck(ctx context.Context, to peer.ID, orig envelope.MsgID) {
	mid, err := envelope.NewMsgID(rand.Reader)
	if err != nil {
		m.ctr.ackSendFailures.Add(1)
		return
	}
	frame, err := envelope.Encode(envelope.Envelope{
		MsgID:   mid,
		Type:    envelope.TypeAck,
		Payload: orig[:],
	})
	if err != nil {
		m.ctr.ackSendFailures.Add(1)
		return
	}
	if err := m.sendControl(ctx, to, frame); err != nil {
		// Потерянный ack не фатален: пир ретраит, дедуп переответит.
		m.ctr.ackSendFailures.Add(1)
		return
	}
	m.ctr.acksSent.Add(1)
}

// backoff — экспоненциальная пауза после attempts попыток: 5с → 1ч (cap).
func backoff(attempts int) time.Duration {
	d := retryBase
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= retryCap {
			return retryCap
		}
	}
	return d
}
