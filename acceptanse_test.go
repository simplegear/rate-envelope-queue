package rate_envelope_queue

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/PavelAgarkov/rate-envelope-queue/provider"
	"k8s.io/component-base/metrics/legacyregistry"
)

type User struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Age   int    `json:"age"`
}

func Test_Acceptance(t *testing.T) {
	serveMetrics()
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)

	go func() {
		<-sig
		fmt.Println("signal shutdown")
		cancel()
	}()

	user1 := &User{
		Name:  "John",
		Email: "@gmail.com",
		Age:   30,
	}

	redisProvider, err := provider.NewRedisProvider(
		parent,
		provider.RedisConfig{
			Address:  "localhost:6379",
			Username: "",
			Password: "",
			DB:       0,
		},
		"processing_table",
		"fallback_table",
	)
	defer redisProvider.Close()

	emailEnvelope, err := NewEnvelope(
		WithDataProvider(redisProvider),
		WithId(1),
		WithType("email_1"),
		//WithInterval(6*time.Second),
		WithDeadline(5*time.Second),
		WithBeforeHook(func(ctx context.Context, envelope *Envelope) error {
			fmt.Println("hook before email 1 ", envelope.GetId(), time.Now())
			//return ErrStopTask
			return nil
		}),
		WithInvoke(func(ctx context.Context, envelope *Envelope) error {
			time.Sleep(5 * time.Second)
			fmt.Println("📧 Email v1", time.Now())
			user := envelope.GetPayload().(*User)
			fmt.Println("user:", user.Name, user.Email, user.Age)
			user.Name = "Changed Name"
			envelope.UpdatePayload(user)
			return nil
		}),
		WithAfterHook(func(ctx context.Context, envelope *Envelope) error {
			fmt.Println("hook after email 1 ", envelope.GetId(), time.Now())
			user := envelope.GetPayload().(*User)
			fmt.Println("user:", user.Name, user.Email, user.Age)
			// скипнет дальнейшую обработку конверта
			//return ErrStopEnvelope
			//return nil
			// любая ошибка, которая не ErrStopEnvelope будет обработана в failureHook
			return fmt.Errorf("some error after")
		}),
		WithFailureHook(func(ctx context.Context, envelope *Envelope, err error) Decision {
			fmt.Println("failure hook email 1", envelope.GetId(), time.Now(), err)
			//return DefaultOnceDecision()
			//return RetryOnceNowDecision()
			return RetryOnceAfterDecision(time.Second * 5)
			//return nil
		}),
		WithSuccessHook(func(ctx context.Context, envelope *Envelope) {
			fmt.Println("success hook email 1", envelope.GetId(), time.Now())
		}),
		WithStampsPerEnvelope(LoggingStamp()),
		WithPayload(user1),
	)
	if err != nil {
		t.Fatal(err)
	}

	emailEnvelope1, err := NewEnvelope(
		WithId(1),
		WithType("email_2"),
		WithScheduleModeInterval(5*time.Second),
		WithDeadline(3*time.Second),
		WithInvoke(func(ctx context.Context, envelope *Envelope) error {
			time.Sleep(5 * time.Second)
			fmt.Println("📧 Email v2", time.Now())
			return nil
		}),
		WithBeforeHook(func(ctx context.Context, envelope *Envelope) error {
			fmt.Println("hook before email 2", envelope.GetId(), time.Now())
			//return ErrStopTask
			return nil
		}),
		WithAfterHook(func(ctx context.Context, envelope *Envelope) error {
			fmt.Println("hook after email 2", envelope.GetId(), time.Now())
			return ErrStopEnvelope
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	metricsEnvelope, err := NewEnvelope(
		WithId(2),
		WithType("metrics"),
		WithScheduleModeInterval(3*time.Second),
		WithDeadline(1*time.Second),
		WithBeforeHook(func(ctx context.Context, envelope *Envelope) error {
			return nil
		}),
		WithInvoke(func(ctx context.Context, envelope *Envelope) error {
			fmt.Println("📧 Metrics v2", time.Now())
			return nil
		}),
		WithAfterHook(func(ctx context.Context, envelope *Envelope) error {
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	metricsEnvelope1, err := NewEnvelope(
		WithId(2),
		WithType("metrics_1"),
		WithScheduleModeInterval(0),
		WithDeadline(1*time.Second),
		WithBeforeHook(func(ctx context.Context, envelope *Envelope) error {
			return nil
		}),
		WithInvoke(func(ctx context.Context, envelope *Envelope) error {
			fmt.Println("📧 Metrics v1", time.Now())
			return nil
		}),
		WithAfterHook(func(ctx context.Context, envelope *Envelope) error {
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	metricsEnvelope3, err := NewEnvelope(
		WithId(2),
		WithType("metrics_3"),
		WithScheduleModeInterval(0),
		WithDeadline(1*time.Second),
		WithBeforeHook(func(ctx context.Context, envelope *Envelope) error {
			return nil
		}),
		WithInvoke(func(ctx context.Context, envelope *Envelope) error {
			fmt.Println("📧 Metrics v3", time.Now())
			return errors.New("some error")
		}),
		WithAfterHook(func(ctx context.Context, envelope *Envelope) error {
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	foodEnvelope, err := NewEnvelope(
		WithId(3),
		WithType("food"),
		WithScheduleModeInterval(2*time.Second),
		WithDeadline(1*time.Second),
		WithBeforeHook(func(ctx context.Context, envelope *Envelope) error {
			return nil
		}),
		WithInvoke(func(ctx context.Context, envelope *Envelope) error {
			fmt.Println("📧Fooding v2", time.Now())
			return nil
		}),
		WithAfterHook(func(ctx context.Context, envelope *Envelope) error {
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	envelops := map[string]*Envelope{
		"Email":   emailEnvelope,
		"Metrics": metricsEnvelope,
		"Food":    foodEnvelope,
	}

	envelopeQueue := NewRateEnvelopeQueue(
		parent,
		"test_queue",
		WithLimitOption(5),
		WithWaitingOption(true),
		WithStopModeOption(Drain),
		WithAllowedCapacityOption(50),
	)

	start := func() {
		envelopeQueue.Start()
		err := envelopeQueue.Send(envelops["Email"], envelops["Metrics"], envelops["Food"])
		if err != nil {
			fmt.Println("add err:", err)
		}
		err = envelopeQueue.Send(emailEnvelope1)
		if err != nil {
			fmt.Println("add err:", err)
		}
		err = envelopeQueue.Send(emailEnvelope, emailEnvelope1, metricsEnvelope1, metricsEnvelope3)
		if err != nil {
			fmt.Println("add err:", err)
		}
	}
	stop := func() {
		envelopeQueue.Stop()
	}

	start()
	stop()
	//envelopeQueue.Terminate()
	err = envelopeQueue.Send(foodEnvelope)
	if err != nil {
		fmt.Println("add err after stop:", err)
	}
	start()

	go func() {
		for i := range 15 {
			user1 := &User{
				Name:  "John",
				Email: "@gmail.com",
				Age:   30,
			}
			id := i + 1
			emailEnvelope, err = NewEnvelope(
				WithId(uint64(id)),
				WithType("email_1"),
				//WithInterval(6*time.Second),
				WithDeadline(5*time.Second),
				WithBeforeHook(func(ctx context.Context, envelope *Envelope) error {
					fmt.Println("hook before email 1 ", envelope.GetId(), time.Now())
					//return ErrStopTask
					return nil
				}),
				WithInvoke(func(ctx context.Context, envelope *Envelope) error {
					time.Sleep(5 * time.Second)
					fmt.Println("📧 Email v1", time.Now())
					user := envelope.GetPayload().(*User)
					fmt.Println("user:", user.Name, user.Email, user.Age)
					user.Name = "Changed Name"
					envelope.UpdatePayload(user)
					return nil
				}),
				WithAfterHook(func(ctx context.Context, envelope *Envelope) error {
					fmt.Println("hook after email 1 ", envelope.GetId(), time.Now())
					user := envelope.GetPayload().(*User)
					fmt.Println("user:", user.Name, user.Email, user.Age)
					// скипнет дальнейшую обработку конверта
					//return ErrStopEnvelope
					//return nil
					// любая ошибка, которая не ErrStopEnvelope будет обработана в failureHook
					return fmt.Errorf("some error after")
				}),
				WithFailureHook(func(ctx context.Context, envelope *Envelope, err error) Decision {
					fmt.Println("failure hook email 1", envelope.GetId(), time.Now(), err)
					return DefaultOnceDecision()
					//return RetryOnceNowDecision()
					//return RetryOnceAfterDecision(time.Second * 5)
					//return nil
				}),
				WithSuccessHook(func(ctx context.Context, envelope *Envelope) {
					fmt.Println("success hook email 1", envelope.GetId(), time.Now())
				}),
				WithStampsPerEnvelope(LoggingStamp()),
				WithPayload(user1),
			)
			if err != nil {
				t.Fatal(err)
			}
			err = envelopeQueue.Send(emailEnvelope)
			if err != nil {
				fmt.Println("add err async:", err)
			}
		}
	}()

	go func() {
		select {
		case <-time.After(25 * time.Second):
			fmt.Println("main: timeout")
			cancel()
		}
	}()

	<-parent.Done()
	fmt.Println("parent: done")
	stop()
	fmt.Println("queue: done")
}

func serveMetrics() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", legacyregistry.Handler())
	go http.ListenAndServe(":8080", mux)
}
