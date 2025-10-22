package rate_envelope_queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/client-go/util/workqueue"
	_ "k8s.io/component-base/metrics/prometheus/workqueue"
)

type QueueState int32

const (
	StateInit     QueueState = iota // создана, ещё не стартовала; Add() — буферизуется
	StateRunning                    // работает; Add() — сразу в workqueue
	StateStopping                   // идёт останов; Add() — ошибка
	StateStopped                    // остановлена; Add() — ошибка; возможен повторный Start()
	StateTerminate
)

const (
	hardHookLimit = 2000 * time.Millisecond
	frac          = 0.9
)

type RateEnvelopeQueue struct {
	name string

	terminateCtx    context.Context
	terminateCancel context.CancelFunc

	runCtx    context.Context
	runCancel context.CancelFunc

	limit         int
	queueMu       sync.RWMutex
	queue         workqueue.TypedRateLimitingInterface[*Envelope]
	limiter       workqueue.TypedRateLimiter[*Envelope]
	workqueueConf *workqueue.TypedRateLimitingQueueConfig[*Envelope]

	waiting bool
	wg      sync.WaitGroup

	stopMode StopMode

	run atomic.Bool // быстрый флаг «жива ли очередь» для воркеров при перепланировании

	// защита старт/стоп/смена очереди/смена состояния
	lifecycleMu sync.Mutex
	// защита только чтения состояния
	stateMu sync.RWMutex
	state   QueueState

	queueStamps []Stamp // глобальные stamps очереди

	// Буфер задач, добавленных до первого Start()
	pendingMu sync.Mutex
	pending   []*Envelope

	allowedCapacity uint64
	currentCapacity atomic.Uint64
}

// NewRateEnvelopeQueue по умолчанию workqueue теряет задачи в режиме AddAfter при любой остановке очереди.
// ----------------------------------------------------------------------------------
// аккуратный режим остановки, прочитаем все что в очереди
// WithWaitingOption(true),
// WithStopModeOption(Drain),
// ----------------------------------------------------------------------------------
// корректно, если нужен «почти drain», но без жёсткого ожидания всех воркеров
// WithWaitingOption(false),
// WithStopModeOption(Drain),
// ----------------------------------------------------------------------------------
// корректно для «быстрого, но чистого» останова с ожиданием
// WithWaitingOption(true),
// WithStopModeOption(Stop),
// ----------------------------------------------------------------------------------
// мгновернный останов без ожидания, все теряем
// WithWaitingOption(false),
// WithStopModeOption(Stop),
// ----------------------------------------------------------------------------------
func NewRateEnvelopeQueue(base context.Context, name string, options ...func(*RateEnvelopeQueue)) SingleQueuePool {
	terminateCtx, cancel := context.WithCancel(base)
	q := &RateEnvelopeQueue{
		terminateCtx:    terminateCtx,
		terminateCancel: cancel,
		waiting:         true,
		state:           StateInit,
		name:            name,
	}
	for _, o := range options {
		o(q)
	}

	if q.limiter == nil {
		q.limiter = workqueue.NewTypedMaxOfRateLimiter[*Envelope]()
	}
	if q.limit <= 0 {
		panic(fmt.Sprintf("%s - queue name %s : invalid limit %d", service, q.name, q.limit))
	}
	if q.stopMode == "" {
		panic(fmt.Sprintf("%s - queue name %s : no stop mode", service, q.name))
	}
	// в init очередь ещё не «живая»
	q.run.Store(false)

	return q
}

func NewSimpleDrainQueue(base context.Context, queueName string, queueRate int) SingleQueuePool {
	q := NewRateEnvelopeQueue(
		base,
		queueName,
		WithLimitOption(queueRate),
		WithWaitingOption(true),
		WithStopModeOption(Drain),
		WithAllowedCapacityOption(1_000_000),
	)
	return q
}

func NewSimpleStopQueue(base context.Context, queueName string, queueRate int) SingleQueuePool {
	q := NewRateEnvelopeQueue(
		base,
		queueName,
		WithLimitOption(queueRate),
		WithWaitingOption(false),
		WithStopModeOption(Stop),
		WithAllowedCapacityOption(1_000_000),
	)
	return q
}

func chain(base Invoker, stamps ...Stamp) Invoker {
	w := base
	for i := len(stamps) - 1; i >= 0; i-- {
		w = stamps[i](w)
	}
	return w
}

func (q *RateEnvelopeQueue) buildInvokerChain(e *Envelope) Invoker {
	base := func(ctx context.Context, env *Envelope) error {
		return env.invoke(ctx, env)
	}
	return chain(base, append(q.queueStamps, e.stamps...)...)
}

func (q *RateEnvelopeQueue) CurrentState() QueueState {
	q.stateMu.RLock()
	s := q.state
	q.stateMu.RUnlock()
	return s
}

func (q *RateEnvelopeQueue) setState(s QueueState) {
	q.stateMu.Lock()
	q.state = s
	q.stateMu.Unlock()
}

func (q *RateEnvelopeQueue) worker(ctx context.Context) {
	if q.waiting {
		defer q.wg.Done()
	}
	q.queueMu.RLock()
	queue := q.queue
	q.queueMu.RUnlock()
	if queue == nil {
		log.Printf(fmt.Sprintf("%s - queue %s : no queue %s", service, q.name, q.name))
		return
	}

	for {
		envelope, shutdown := queue.Get()
		if shutdown {
			log.Printf(fmt.Sprintf("%s - queue %s : worker is shutting down", service, q.name))
			return
		}

		err := func(envelope *Envelope) error {
			defer q.dec()
			defer queue.Done(envelope)
			defer func() {
				if r := recover(); r != nil {
					// важен порядок: Forget до выхода (Done отработает после этого defer)
					queue.Forget(envelope)
					log.Printf(fmt.Sprintf("%s - queue %s : recovered from panic: %v\n%s", service, q.name, r, debug.Stack()))
				}
			}()

			tctx := ctx
			var tcancel context.CancelFunc = func() {}
			if envelope.deadline > 0 {
				tctx, tcancel = context.WithTimeout(ctx, envelope.deadline)
			}
			defer tcancel()

			var important error = nil
			if envelope.beforeHook != nil {
				hctx, cancel := withHookTimeout(tctx, envelope.deadline, frac, hardHookLimit)
				important = envelope.beforeHook(hctx, envelope)
				cancel()
				if important != nil {
					if errors.Is(important, ErrStopEnvelope) && envelope.afterHook != nil {
						hctx, cancel := withHookTimeout(tctx, envelope.deadline, frac, hardHookLimit)
						_ = envelope.afterHook(hctx, envelope) // ignore result
						cancel()
					}
					goto handle
				}
			}

			{
				invoker := q.buildInvokerChain(envelope)
				important = invoker(tctx, envelope)
				if important != nil {
					if errors.Is(important, ErrStopEnvelope) && envelope.afterHook != nil {
						hctx, cancel := withHookTimeout(tctx, envelope.deadline, frac, hardHookLimit)
						_ = envelope.afterHook(hctx, envelope) // ignore result
						cancel()
					}
					goto handle
				}
			}

			if envelope.afterHook != nil {
				hctx, cancel := withHookTimeout(tctx, envelope.deadline, frac, hardHookLimit)
				important = envelope.afterHook(hctx, envelope)
				cancel()
				if important != nil && !errors.Is(important, ErrStopEnvelope) {
					important = nil // игнорим всё, кроме стопа
				}
				goto handle
			}

		handle:
			if important == nil && tctx.Err() != nil {
				important = tctx.Err()
			}

			switch {
			// ветвь стала не актуальна т.к. перенесена в общий кейс important != nil
			//т.к отмена контеста или дедлайн — это тоже ошибка выполнения задачи
			// и пользователь может на неё реагировать в failureHook
			//(например, если задача одиночная и нужно уведомить пользователя)
			//case errors.Is(important, context.Canceled) || errors.Is(important, context.DeadlineExceeded):

			// ErrStopEnvelope — забыть и не перепланировать. Ошибка от пользователя о том, что задача больше не нужна
			case errors.Is(important, ErrStopEnvelope):
				queue.Forget(envelope)

				return nil

			// любая ошибка(в том числе и errors.Is(important, context.DeadlineExceeded) || errors.Is(important, context.Canceled)) — перепланировать
			//(если периодическая и очередь жива) и, если одиночная, вызвать failureHook (если есть) и реагировать
			//на ответ пользователя через DestinationResult
			case important != nil:
				queue.Forget(envelope)

				alive := q.isAlive(queue)
				if envelope.interval > 0 && alive {
					queue.AddAfter(envelope, envelope.interval)
					q.inc(1)
				}

				// одиночная задача с ошибкой — срабатывает failureHook (если есть) и забывается
				if envelope.interval == 0 && envelope.failureHook != nil {
					// на этот хук даем общее время дедлайна, важная часть. Нужно дать возомжность отправить
					//ошибку в сторонний сервис. Снаружи пользователь управляет временем через deadline
					hctx, cancel := withHookTimeout(tctx, envelope.deadline, frac, hardHookLimit)
					decision := envelope.failureHook(hctx, envelope, important)
					cancel()

					if decision == nil {
						decision = DefaultOnceDecision()
					}

					payload := decision.Payload()

					state, ok := payload[DestinationStateField].(destinationState)
					if !ok {
						payload[DestinationStateField] = DecisionStateDrop
						state = DecisionStateDrop
					}

					alive := q.isAlive(queue)
					if alive {
						switch state {
						case DecisionStateRetryNow:
							queue.Add(envelope)
							q.inc(1)
						case DecisionStateRetryAfter:
							delay, ok := payload[PayloadAfterField]
							if !ok {
								log.Printf(fmt.Sprintf("%s - queue name %s - envelope %s/%d : failureHook did not return after field; using 30s, please customize field", service, q.name, envelope._type, envelope.id))
								delay = 30 * time.Second
							}

							delayInterval, assert := delay.(time.Duration)
							if !assert {
								log.Printf(fmt.Sprintf("%s - queue name %s - envelope %s/%d : failureHook returned invalid after field; using 30s, please customize field", service, q.name, envelope._type, envelope.id))
								delayInterval = 30 * time.Second
							}

							if delayInterval > 0 {
								queue.AddAfter(envelope, delayInterval)
								q.inc(1)
							}
						case DecisionStateDrop:
						}
					}
				}
				return nil

			default:
				queue.Forget(envelope)

				alive := q.isAlive(queue)
				if envelope.interval > 0 && alive {
					queue.AddAfter(envelope, envelope.interval)
					q.inc(1)
				}
				if envelope.successHook != nil {
					hctx, cancel := withHookTimeout(tctx, envelope.deadline, frac, hardHookLimit)
					envelope.successHook(hctx, envelope)
					cancel()
				}
				return nil
			}
		}(envelope)

		if err != nil {
			log.Printf(fmt.Sprintf("%s - queue %s : worker envelope processing error %s/%d: %v", service, q.name, envelope._type, envelope.id, err))
		}
	}
}

func (q *RateEnvelopeQueue) isAlive(queue workqueue.TypedRateLimitingInterface[*Envelope]) bool {
	return q.run.Load() && q.runCtx != nil && q.runCtx.Err() == nil &&
		q.CurrentState() == StateRunning && !queue.ShuttingDown()
}

func (q *RateEnvelopeQueue) Send(envelopes ...*Envelope) error {
	// валидация содержимого (не состояния)
	for _, e := range envelopes {
		if e == nil {
			return ErrAdditionEnvelopeToQueueBadFields
		}
		if e.invoke == nil || e.interval < 0 || e.deadline < 0 {
			return ErrAdditionEnvelopeToQueueBadFields
		}
		if e.interval > 0 && e.deadline > e.interval {
			return ErrAdditionEnvelopeToQueueBadIntervals
		}
	}

	need := uint64(len(envelopes))

	for {
		s := q.CurrentState()
		switch s {
		case StateTerminate:
			return ErrQueueIsTerminated
		//case StateInit, stateStopped:
		case StateInit:
			q.pendingMu.Lock()
			// повторная проверка состояния под локом
			//if q.currentState() == StateInit || q.currentState() == stateStopped {
			if q.CurrentState() == StateInit {
				// в init/stopped — буферизуем, если есть место
				// (в stopped — на случай, если очередь остановлена и потом снова запущена)
				// в stopped буфер не чистим, т.к. может быть повторный старт
				// проверяем вместимость
				if !q.tryReserve(need) {
					q.pendingMu.Unlock()
					return ErrAllowedQueueCapacityExceeded
				}
				q.pending = append(q.pending, envelopes...)
				q.pendingMu.Unlock()
				return nil
			}
			q.pendingMu.Unlock()
			// состояние сменилось — пробуем снова по новому пути
			continue

		case StateRunning:
			if !q.tryReserve(need) {
				return ErrAllowedQueueCapacityExceeded
			}

			q.lifecycleMu.Lock()
			// повторная проверка под той же блокировкой, что и Stop/Start
			if q.CurrentState() != StateRunning {
				q.lifecycleMu.Unlock()
				q.unreserve(need)
				return ErrEnvelopeQueueIsNotRunning
			}

			q.queueMu.RLock()
			local := q.queue
			shutting := local == nil || local.ShuttingDown()
			q.queueMu.RUnlock()
			if shutting {
				q.lifecycleMu.Unlock()
				q.unreserve(need)
				return ErrEnvelopeQueueIsNotRunning
			}

			for _, e := range envelopes {
				local.Add(e)
			}
			q.lifecycleMu.Unlock()
			return nil

		case StateStopping, StateStopped:
			return ErrEnvelopeQueueIsNotRunning

		default:
			return ErrEnvelopeQueueIsNotRunning
		}
	}
}

func (q *RateEnvelopeQueue) Start() {
	q.lifecycleMu.Lock()
	defer q.lifecycleMu.Unlock()

	switch q.CurrentState() {
	case StateTerminate:
		log.Printf(fmt.Sprintf("%s - queue %s : cannot start terminated queue", service, q.name))
		return
	case StateRunning:
		return
	case StateStopping:
		log.Printf(fmt.Sprintf("%s - queue %s : cannot start stopping queue", service, q.name))
		return
	}

	// recreate workqueue
	var newQ workqueue.TypedRateLimitingInterface[*Envelope]
	if q.workqueueConf == nil {
		newQ = workqueue.NewTypedRateLimitingQueueWithConfig[*Envelope](q.limiter, workqueue.TypedRateLimitingQueueConfig[*Envelope]{Name: q.name})
	} else {
		q.workqueueConf.Name = q.name
		newQ = workqueue.NewTypedRateLimitingQueueWithConfig[*Envelope](q.limiter, *q.workqueueConf)
	}

	q.queueMu.Lock()
	q.queue = newQ
	q.queueMu.Unlock()

	// переключаем состояние и run-флаг
	q.setState(StateRunning)
	q.run.Store(true)

	q.runCtx, q.runCancel = context.WithCancel(q.terminateCtx)

	// запустить воркеры
	for i := 0; i < q.limit; i++ {
		if q.waiting {
			q.wg.Add(1)
		}
		go func() {
			defer recoverWrap()
			q.worker(q.runCtx)
		}()
	}

	// слить pending
	q.pendingMu.Lock()
	if len(q.pending) > 0 {
		q.queueMu.RLock()
		local := q.queue
		q.queueMu.RUnlock()
		for _, e := range q.pending {
			local.Add(e)
		}
		q.pending = nil
	}
	q.pendingMu.Unlock()
}

func (q *RateEnvelopeQueue) Stop() {
	// Переводим состояние в stopping (под "зонтиком"), без долгих операций под локом.
	q.lifecycleMu.Lock()
	if q.CurrentState() != StateRunning {
		q.lifecycleMu.Unlock()
		return
	}
	q.setState(StateStopping)
	q.run.Store(false)
	runCancel := q.runCancel

	q.lifecycleMu.Unlock()

	if runCancel != nil {
		runCancel()
	}

	// Снимок ссылки на очередь (не обнуляем публикацию до финализации).
	q.queueMu.RLock()
	local := q.queue
	q.queueMu.RUnlock()

	// Сигнал остановки очереди.
	if local != nil {
		switch q.stopMode {
		case Drain:
			local.ShutDownWithDrain()
		default: // Stop
			local.ShutDown()
		}
	}

	// Если ждём — дождаться завершения воркеров
	if q.waiting {
		q.wg.Wait()

		// Пост-коррекция ёмкости: снимаем "хвост" (queued/delayed), который мог потеряться при остановке.
		// Оставляем резерв только под pending, т.к. они переживают рестарт.
		q.pendingMu.Lock()
		pend := uint64(len(q.pending))
		q.pendingMu.Unlock()

		cur := q.currentCapacity.Load()
		if cur > pend {
			q.unreserve(cur - pend)
		}
	} else {
		// waiting=false: не трогаем счётчик — in-flight сами вызовут dec() позже.
		// Для Stop хвост остаётся учтённым (задокументированная утечка до следующего корректного цикла).
	}

	// Финализируем состояние и публикуем отсутствие очереди.
	q.lifecycleMu.Lock()
	q.setState(StateStopped)
	q.lifecycleMu.Unlock()

	q.queueMu.Lock()
	q.queue = nil
	q.queueMu.Unlock()

	q.runCtx = nil
	q.runCancel = nil

	log.Printf(fmt.Sprintf("%s - queue %s : stopped", service, q.name))
}

func (q *RateEnvelopeQueue) Drain() {
	if q.stopMode != Drain {
		return
	}
	// Разрешаем Drain только из Running-состояния.
	q.lifecycleMu.Lock()
	if q.CurrentState() != StateRunning {
		q.lifecycleMu.Unlock()
		return
	}

	q.run.Store(false)

	// Снимок ссылки на очередь под тем же «зонтиком».
	q.queueMu.RLock()
	local := q.queue
	q.queueMu.RUnlock()
	q.lifecycleMu.Unlock()

	if local != nil {
		local.ShutDownWithDrain()
	}

	if q.waiting {
		q.wg.Wait()

		q.pendingMu.Lock()
		pend := uint64(len(q.pending))
		q.pendingMu.Unlock()

		cur := q.currentCapacity.Load()
		if cur > pend {
			q.unreserve(cur - pend)
		}
	}

	log.Printf(fmt.Sprintf("%s - queue %s : drained", service, q.name))
}

func (q *RateEnvelopeQueue) Terminate() {
	q.lifecycleMu.Lock()
	if q.CurrentState() != StateStopped {
		q.lifecycleMu.Unlock()
		return
	}
	q.setState(StateTerminate)
	termCancel := q.terminateCancel
	q.lifecycleMu.Unlock()

	if termCancel != nil {
		termCancel()
	}

	return
}

func (q *RateEnvelopeQueue) tryReserve(n uint64) bool {
	for {
		cur := q.currentCapacity.Load()
		if q.allowedCapacity != 0 && cur+n > q.allowedCapacity {
			return false
		}
		if q.currentCapacity.CompareAndSwap(cur, cur+n) {
			return true
		}
	}
}

func (q *RateEnvelopeQueue) unreserve(n uint64) {
	q.currentCapacity.Add(^uint64(n - 1))
}

func (q *RateEnvelopeQueue) inc(n uint64) {
	q.currentCapacity.Add(n)
}

func (q *RateEnvelopeQueue) dec() {
	q.currentCapacity.Add(^uint64(0))
}

func WithLimitOption(limit int) func(*RateEnvelopeQueue) {
	return func(q *RateEnvelopeQueue) {
		q.limit = limit
	}
}

func WithWaitingOption(waiting bool) func(*RateEnvelopeQueue) {
	return func(q *RateEnvelopeQueue) {
		q.waiting = waiting
	}
}

func WithStopModeOption(mode StopMode) func(*RateEnvelopeQueue) {
	return func(q *RateEnvelopeQueue) {
		if mode != Drain && mode != Stop {
			panic("invalid stop mode")
		}
		q.stopMode = mode
	}
}

func WithWorkqueueConfigOption(conf *workqueue.TypedRateLimitingQueueConfig[*Envelope]) func(*RateEnvelopeQueue) {
	return func(q *RateEnvelopeQueue) {
		if conf != nil {
			c := *conf
			q.workqueueConf = &c
		}
	}
}

func WithLimiterOption(limiter workqueue.TypedRateLimiter[*Envelope]) func(*RateEnvelopeQueue) {
	return func(q *RateEnvelopeQueue) {
		if limiter != nil {
			q.limiter = limiter
		}
	}
}

func WithStamps(stamps ...Stamp) func(*RateEnvelopeQueue) {
	return func(q *RateEnvelopeQueue) {
		q.queueStamps = append(q.queueStamps, stamps...)
	}
}

func WithAllowedCapacityOption(capacity uint64) func(*RateEnvelopeQueue) {
	return func(q *RateEnvelopeQueue) {
		q.allowedCapacity = capacity
	}
}
