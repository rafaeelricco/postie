// Package delivery contains destination independent retry and delivery
// orchestration. HTTP request construction lives in an adapter.
package delivery

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"time"

	"github.com/rafaeelricco/postie/internal/protocol"
	"github.com/rafaeelricco/postie/internal/stream"
)

// Processor delivers records to one destination, retrying until a terminal
// outcome. Sleep and Jitter are fields so tests can remove time and randomness.
type Processor struct {
	Sender      AttemptSender
	Destination Destination
	Sleep       func(context.Context, time.Duration) error
	Jitter      func(int64) int64
}

// NewProcessor returns a Processor that really sleeps and really jitters.
func NewProcessor(destination Destination, sender AttemptSender) *Processor {
	return &Processor{Sender: sender, Destination: destination, Sleep: ctxSleep, Jitter: rand.Int64N}
}

// Process sends one record until the destination gives a terminal answer.
// There is no attempt limit: the only ways out are an outcome or a cancelled
// context. onAttempt, when not nil, observes every attempt (0-based) and its
// error.
//
//	outcome, err := p.Process(ctx, record, func(attempt int, err error) {
//		log.Printf("attempt %d: %v", attempt+1, err)
//	})
func (p *Processor) Process(ctx context.Context, record stream.Record, onAttempt func(int, error)) (Outcome, error) {
	if !protocol.MatchesFilter(p.Destination.Filter, record.Payload) {
		return Filtered, nil
	}
	body, err := json.Marshal(envelopeFor(record, p.Destination))
	if err != nil {
		return 0, err
	}
	for attempt := 0; ; attempt++ {
		outcome, err := p.Sender.Send(ctx, record, body)
		if onAttempt != nil {
			onAttempt(attempt, err)
		}
		if err == nil {
			return outcome, nil
		}
		if err := p.Sleep(ctx, backoff(attempt, p.Jitter)); err != nil {
			return 0, err
		}
	}
}

// envelopeFor addresses a record's payload to a destination.
func envelopeFor(record stream.Record, destination Destination) protocol.Envelope {
	return protocol.Envelope{
		DataSourceID:               record.Source.ID,
		DataSourceDescription:      record.Source.Description,
		DataDestinationID:          destination.ID,
		DataDestinationDescription: destination.Description,
		Payload:                    record.Payload,
	}
}

// Outcome is how delivery of one record ended. Every outcome is terminal: the
// record's offset may be committed once one is reached. The zero value is not
// an outcome and only accompanies an error.
type Outcome int

const (
	// Delivered means the destination acknowledged the event.
	Delivered Outcome = iota + 1
	// Filtered means the destination's filter excluded the event, so nothing
	// was sent.
	Filtered
	// Skipped means the destination rejected the event for good ("keep_going").
	// A skip is recorded durably before the offset is committed.
	Skipped
)

// String is the outcome name used in activity entries.
func (o Outcome) String() string {
	switch o {
	case Delivered:
		return "delivered"
	case Filtered:
		return "filtered"
	case Skipped:
		return "skipped"
	default:
		return ""
	}
}

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
