package rate_envelope_queue

import (
	"context"
	"errors"
	"time"

	"github.com/PavelAgarkov/rate-envelope-queue/provider"
)

type Envelope struct {
	id    uint64
	_type string

	internalId string
	fallbackId string

	interval time.Duration
	deadline time.Duration

	// ----- хуки, который разрешают вмешиваться в процесс обработки конверта и остановить его через ошибку ErrStopEnvelope
	beforeHook func(ctx context.Context, envelope *Envelope) error
	invoke     func(ctx context.Context, envelope *Envelope) error
	afterHook  func(ctx context.Context, envelope *Envelope) error
	// -----
	// хук, которй позволяет динамически реагировать по решению пользователя на поведение конверта при ошибке
	failureHook func(ctx context.Context, envelope *Envelope, err error) Decision
	successHook func(ctx context.Context, envelope *Envelope)

	stamps  []Stamp // per-envelope stamps
	payload interface{}

	dataProvider provider.DataProvider
}

func NewDynamicEnvelope(deadline time.Duration, invoke Invoker, payload interface{}) (*Envelope, error) {
	envelope, err := NewEnvelope(
		WithDeadline(deadline),
		WithInvoke(invoke),
		WithScheduleModeInterval(0),
		WithPayload(payload),
	)
	return envelope, err
}

func NewScheduleEnvelope(interval, deadline time.Duration, invoke Invoker, payload interface{}) (*Envelope, error) {
	envelope, err := NewEnvelope(
		WithDeadline(deadline),
		WithInvoke(invoke),
		WithScheduleModeInterval(interval),
		WithPayload(payload),
	)
	return envelope, err
}

func NewEnvelope(opt ...func(*Envelope)) (*Envelope, error) {
	envelope := &Envelope{}
	for _, o := range opt {
		o(envelope)
	}

	// validate required fields
	if envelope.invoke == nil {
		return nil, errors.New("envelope invoke is nil")
	}

	return envelope, nil
}

func (envelope *Envelope) GetId() uint64 {
	return envelope.id
}

func (envelope *Envelope) GetType() string {
	return envelope._type
}

func (envelope *Envelope) GetStamps() []Stamp {
	return envelope.stamps
}

func (envelope *Envelope) GetPayload() interface{} {
	return envelope.payload
}

func (envelope *Envelope) UpdatePayload(p interface{}) {
	envelope.payload = p
}

func WithStampsPerEnvelope(stamps ...Stamp) func(*Envelope) {
	return func(e *Envelope) {
		e.stamps = append(e.stamps, stamps...)
	}
}

func WithBeforeHook(hook Invoker) func(*Envelope) {
	return func(e *Envelope) {
		e.beforeHook = hook
	}
}

func WithAfterHook(hook Invoker) func(*Envelope) {
	return func(e *Envelope) {
		e.afterHook = hook
	}
}

func WithInvoke(invoke Invoker) func(*Envelope) {
	return func(e *Envelope) {
		e.invoke = invoke
	}
}

func WithFailureHook(hook func(ctx context.Context, envelope *Envelope, err error) Decision) func(*Envelope) {
	return func(e *Envelope) {
		e.failureHook = hook
	}
}

func WithSuccessHook(hook func(ctx context.Context, envelope *Envelope)) func(*Envelope) {
	return func(e *Envelope) {
		e.successHook = hook
	}
}

// WithScheduleModeInterval 0 means run once, not a schedule
func WithScheduleModeInterval(d time.Duration) func(*Envelope) {
	return func(e *Envelope) {
		e.interval = d
	}
}

func WithDeadline(d time.Duration) func(*Envelope) {
	return func(e *Envelope) {
		e.deadline = d
	}
}

func WithType(t string) func(*Envelope) {
	return func(e *Envelope) {
		e._type = t
	}
}

func WithId(id uint64) func(*Envelope) {
	return func(e *Envelope) {
		e.id = id
	}
}

func WithPayload(p interface{}) func(*Envelope) {
	return func(e *Envelope) {
		e.payload = p
	}
}

func WithDataProvider(dp provider.DataProvider) func(*Envelope) {
	return func(e *Envelope) {
		e.dataProvider = dp
	}
}
