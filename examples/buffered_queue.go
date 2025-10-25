package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	req "github.com/simplegear/rate-envelope-queue"
)

func main() {
	log.Printf("Starting buffered rate envelope queue example now %s", time.Now().Format(time.RFC3339))
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)

	go func() {
		<-sig
		fmt.Println("signal shutdown")
		cancel()
	}()

	queue := req.NewRateEnvelopeQueue(
		parent,
		"some_queue",
		req.WithLimitOption(3),
		req.WithWaitingOption(false), // always true in stop mode 'Drain' and 'Stop'
		req.WithStopModeOption(req.Drain),
		//req.WithStopModeOption(req.Stop),
	)

	envelope1, _ := req.NewEnvelope(
		req.WithType("task_type_1"),
		req.WithId(1),
		req.WithInvoke(func(ctx context.Context, envelope *req.Envelope) error {
			fmt.Println("envelope 1: invoked")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
			}
			fmt.Println("envelope 1: completed")
			return nil
		}),
		req.WithDeadline(20*time.Second),
		req.WithScheduleModeInterval(0),
	)

	envelope2, _ := req.NewEnvelope(
		req.WithType("task_type_2"),
		req.WithId(2),
		req.WithInvoke(func(ctx context.Context, envelope *req.Envelope) error {
			fmt.Println("envelope 2: invoked")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(7 * time.Second):
			}
			fmt.Println("envelope 2: completed")
			return nil
		}),
		req.WithDeadline(20*time.Second),
		req.WithScheduleModeInterval(0),
	)

	_ = queue.Send(envelope1, envelope2)

	queue.Start()
	queue.Stop()
	queue.Terminate()
}
