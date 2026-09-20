package delivery

import (
	"context"
	"math/rand/v2"
	"time"
)

type Processor struct {
	Sender      AttemptSender
	Destination Destination
	Sleep       func(context.Context, time.Duration) error
	Jitter      func(int64) int64
}

func NewProcessor(destination Destination, sender AttemptSender) *Processor {
	return &Processor{Sender: sender, Destination: destination, Sleep: ctxSleep, Jitter: rand.Int64N}
}

func ctxSleep(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func backoff(attempt int, jitter func(int64) int64) time.Duration {
	ceiling := time.Second << min(attempt, 6)
	delay := ceiling/2 + time.Duration(jitter(int64(ceiling/2)+1))
	return min(max(delay, time.Second), 60*time.Second)
}
