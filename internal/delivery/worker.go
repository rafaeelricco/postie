package delivery

import (
	"context"
	"encoding/json"
	"time"

	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/protocol"
	"github.com/rafaeelricco/postie/internal/stream"
)

// storeRetryDelay is the pause between retries of a store or broker call.
const storeRetryDelay = time.Second

// CommitFunc commits the offset of a processed record.
type CommitFunc = func(context.Context, *stream.RawRecord) error

// Worker takes raw records of one source to one destination: decode, deliver,
// record a terminal skip, and commit. Gate and Log are optional.
type Worker struct {
	Processor   *Processor
	Decode      func(*stream.RawRecord) (stream.Record, error)
	Store       SkipStore
	Scope       stream.Scope
	Destination string
	SourceID    string
	Gate        DispatchGate
	Log         *activity.Log
}

// Process finishes one raw record and commits its offset. A nil error means
// the offset was committed. A record that cannot be decoded blocks the whole
// source, because skipping it silently would lose an event.
func (w *Worker) Process(ctx context.Context, raw *stream.RawRecord, commit CommitFunc) error {
	if !w.allowed() {
		return context.Canceled
	}
	record, err := w.Decode(raw)
	if err != nil {
		w.block(err.Error())
		return err
	}
	entry := entryFor(record, w.Destination)
	return completeRecord(ctx, storeRetryDelay, recordActions{
		skipped: func(ctx context.Context) (bool, error) {
			if !raw.Historical {
				return false, nil
			}
			return w.Store.HasSkip(ctx, w.Scope, w.Destination, record)
		},
		send: func(ctx context.Context) (Outcome, error) { return w.send(ctx, record, entry) },
		audit: func(ctx context.Context) error {
			return w.Store.SaveSkip(ctx, w.Scope, w.Destination, record)
		},
		commit: func(ctx context.Context) error { return w.commit(ctx, raw, entry, commit) },
	})
}

// send delivers the record and logs the start, every attempt, and the outcome.
func (w *Worker) send(ctx context.Context, record stream.Record, entry activity.Entry) (Outcome, error) {
	w.log(startedEntry(entry))
	outcome, err := w.Processor.Process(ctx, record, func(attempt int, attemptErr error) {
		w.log(attemptEntry(entry, attempt, attemptErr))
	})
	if err == nil {
		w.log(outcomeEntry(entry, outcome))
	}
	return outcome, err
}

// commit makes one commit attempt and logs it. A failure caused by shutdown is
// not logged: it is expected and will not be retried.
func (w *Worker) commit(ctx context.Context, raw *stream.RawRecord, entry activity.Entry, commit CommitFunc) error {
	if err := commit(ctx, raw); err != nil {
		if ctx.Err() == nil {
			w.log(commitRetryEntry(entry))
		}
		return err
	}
	w.log(committedEntry(entry))
	return nil
}

func (w *Worker) allowed() bool { return w.Gate == nil || w.Gate.Allowed(w.SourceID) }

func (w *Worker) block(reason string) {
	if w.Gate != nil {
		w.Gate.Block(w.SourceID, reason)
	}
}

func (w *Worker) log(entry activity.Entry) {
	if w.Log != nil {
		w.Log.Add(entry)
	}
}

// recordActions are the side effects needed to finish one record. They are
// injected so completeRecord's ordering can be tested without a broker, a
// store, or a destination.
type recordActions struct {
	skipped func(context.Context) (bool, error) // was a terminal skip already recorded?
	send    func(context.Context) (Outcome, error)
	audit   func(context.Context) error // durably record a terminal skip
	commit  func(context.Context) error
}

// completeRecord runs the actions in the only safe order:
//
//  1. skipped: a record whose skip was already recorded is not sent again.
//  2. send:    deliver until a terminal outcome.
//  3. audit:   a Skipped outcome is recorded before the commit, so a crash
//     between the two cannot lose the fact that the event was skipped.
//  4. commit:  only now may the offset advance.
//
// Store and broker calls are retried every delay until they succeed or ctx
// ends. send does its own retrying, so its error is final.
func completeRecord(ctx context.Context, delay time.Duration, actions recordActions) error {
	var skipped bool
	err := Retry(ctx, delay, func(ctx context.Context) error {
		var err error
		skipped, err = actions.skipped(ctx)
		return err
	})
	if err != nil {
		return err
	}
	if !skipped {
		if err := sendAndAudit(ctx, delay, actions); err != nil {
			return err
		}
	}
	return Retry(ctx, delay, actions.commit)
}

func sendAndAudit(ctx context.Context, delay time.Duration, actions recordActions) error {
	outcome, err := actions.send(ctx)
	if err != nil || outcome != Skipped {
		return err
	}
	return Retry(ctx, delay, actions.audit)
}

// The activity entry builders below are pure: each one takes the record's
// base entry by value and returns a modified copy. Error
// texts are fixed categories, never raw dependency errors, so the operator
// log cannot leak payloads or credentials.

// entryFor is the base entry shared by everything logged about one record.
func entryFor(record stream.Record, destination string) activity.Entry {
	aggregateID, eventName := businessIdentifiers(record.Payload)
	return activity.Entry{
		Source: record.Source.ID, Destination: destination, Generation: int(record.Generation),
		Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
		EventID: record.EventID, AggregateID: aggregateID, EventName: eventName,
	}
}

// businessIdentifiers reads the optional aggregate_id and event_name columns
// so operators can search the log by them. Payloads without them yield "".
func businessIdentifiers(payload json.RawMessage) (aggregateID, eventName string) {
	var identifiers struct {
		AggregateID string `json:"aggregate_id"`
		EventName   string `json:"event_name"`
	}
	_ = json.Unmarshal(payload, &identifiers)
	return identifiers.AggregateID, identifiers.EventName
}

func startedEntry(entry activity.Entry) activity.Entry {
	entry.Message = "delivery started"
	return entry
}

// attemptEntry describes one attempt. attempt is 0-based; the log is 1-based.
func attemptEntry(entry activity.Entry, attempt int, err error) activity.Entry {
	entry.Attempt = attempt + 1
	entry.Message = "acknowledged"
	if err != nil {
		entry.Message = "retry"
		entry.Level = "warn"
		entry.Error = "delivery failed or retry requested"
	}
	return entry
}

func outcomeEntry(entry activity.Entry, outcome Outcome) activity.Entry {
	entry.Message = "terminal outcome"
	entry.Outcome = outcome.String()
	return entry
}

func committedEntry(entry activity.Entry) activity.Entry {
	entry.Message = "committed"
	return entry
}

func commitRetryEntry(entry activity.Entry) activity.Entry {
	entry.Level = "warn"
	entry.Message = "commit retry"
	entry.Error = "Kafka commit unavailable"
	return entry
}

// Destination is the part of a destination that shapes the envelope. How to
// reach it (endpoint, credentials) belongs to the AttemptSender.
type Destination struct {
	ID          string
	Description string
	Filter      *protocol.Filter
}

// AttemptSender makes one delivery attempt of an encoded envelope. It returns
// a terminal Outcome, or an error when the attempt should be retried.
type AttemptSender interface {
	Send(context.Context, stream.Record, []byte) (Outcome, error)
}

// SkipStore durably remembers terminal skips per destination. After a crash
// between the skip and the offset commit, the record is redelivered; the
// stored skip stops Postie from sending it to the destination a second time.
type SkipStore interface {
	HasSkip(context.Context, stream.Scope, string, stream.Record) (bool, error)
	SaveSkip(context.Context, stream.Scope, string, stream.Record) error
}

// DispatchGate says whether a source may dispatch right now and lets a worker
// block a source whose records cannot be decoded.
type DispatchGate interface {
	Allowed(source string) bool
	Block(source, reason string)
}
