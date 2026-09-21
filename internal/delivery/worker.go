package delivery

import (
	"context"
	"time"

	"github.com/rafaeelricco/postie/internal/activity"
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
