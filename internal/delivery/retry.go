package delivery

import (
	"context"
	"time"
)

// Retry calls fn until it succeeds, waiting delay between attempts. It gives
// up only when ctx ends, and then returns the context's error rather than
// fn's last one.
//
//	err := delivery.Retry(ctx, time.Second, func(ctx context.Context) error {
//		return store.SaveSkip(ctx, scope, destination, record)
//	})
func Retry(ctx context.Context, delay time.Duration, fn func(context.Context) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if fn(ctx) == nil {
			return nil
		}
		if err := ctxSleep(ctx, delay); err != nil {
			return err
		}
	}
}

// ctxSleep waits for delay, or returns the context's error if ctx ends first.
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

// backoff is the wait before the next attempt: exponential with "equal
// jitter". The ceiling doubles from 1s up to 64s; the delay is half the
// ceiling plus a random share of the other half, clamped to [1s, 60s].
// jitter(n) must return a value in [0, n).
//
//	attempt 0 -> ceiling 1s  -> 1s (clamped up)
//	attempt 3 -> ceiling 8s  -> 4s to 8s
//	attempt 6+ -> ceiling 64s -> 32s to 60s (clamped down)
func backoff(attempt int, jitter func(int64) int64) time.Duration {
	ceiling := time.Second << min(attempt, 6)
	delay := ceiling/2 + time.Duration(jitter(int64(ceiling/2)+1))
	return min(max(delay, time.Second), 60*time.Second)
}
