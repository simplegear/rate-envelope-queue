package rate_envelope_queue

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrStopEnvelope                        = errors.New(fmt.Sprintf("%s: stop envelope", service))
	ErrEnvelopeQueueIsNotRunning           = errors.New(fmt.Sprintf("%s: regect envelope, queue is not running or init", service))
	ErrQueueIsTerminated                   = errors.New(fmt.Sprintf("%s: regect envelope, queue is terminated", service))
	ErrAdditionEnvelopeToQueueBadFields    = errors.New(fmt.Sprintf("%s: addition envelope to queue has bad fields", service))
	ErrAdditionEnvelopeToQueueBadIntervals = errors.New(fmt.Sprintf("%s: addition envelope to queue has bad intervals", service))
	ErrAllowedQueueCapacityExceeded        = errors.New(fmt.Sprintf("%s: allowed queue capacity exceeded", service))

	ErrPassToDataProvider = errors.New(fmt.Sprintf("%s: can't pass envelope to data provider", service))
)

type (
	StopMode string
	Invoker  func(ctx context.Context, envelope *Envelope) error
	Stamp    func(next Invoker) Invoker
)

const (
	Drain StopMode = "drain"
	Stop  StopMode = "stop"

	service = "[rate-envelope-queue]"
)

type (
	SingleQueuePool interface {
		Send(envelopes ...*Envelope) error
		Start()
		Stop()
		Terminate()
		CurrentState() QueueState
	}
)
