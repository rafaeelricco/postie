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
