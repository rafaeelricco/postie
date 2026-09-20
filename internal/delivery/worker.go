package delivery

import (
	"context"
	"encoding/json"
	"time"

	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/stream"
)

type recordActions struct {
	skipped func(context.Context) (bool, error)
	send    func(context.Context) (Outcome, error)
	audit   func(context.Context) error
	commit  func(context.Context) error
}

func completeRecord(ctx context.Context, delay time.Duration, actions recordActions) error {
	var skipped bool
	if err := retry(ctx, delay, func(ctx context.Context) error {
		var err error
		skipped, err = actions.skipped(ctx)
		return err
	}); err != nil {
		return err
	}
	if !skipped {
		outcome, err := actions.send(ctx)
		if err != nil {
			return err
		}
		if outcome == Skipped {
			if err := retry(ctx, delay, actions.audit); err != nil {
				return err
			}
		}
	}
	return retry(ctx, delay, actions.commit)
}

func retry(ctx context.Context, delay time.Duration, fn func(context.Context) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(ctx); err == nil {
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

type SkipStore interface {
	HasSkip(context.Context, stream.Scope, string, stream.Record) (bool, error)
	SaveSkip(context.Context, stream.Scope, string, stream.Record) error
}

type DispatchGate interface {
	Allowed(string) bool
	Block(string, string)
}

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

func (w *Worker) Process(ctx context.Context, raw *stream.RawRecord, commit func(context.Context, *stream.RawRecord) error) error {
	if w.Gate != nil && !w.Gate.Allowed(w.SourceID) {
		return context.Canceled
	}
	record, err := w.Decode(raw)
	if err != nil {
		if w.Gate != nil {
			w.Gate.Block(w.SourceID, err.Error())
		}
		return err
	}
	entry := activity.Entry{
		Source: record.Source.ID, Destination: w.Destination, Generation: int(record.Generation),
		Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, EventID: record.EventID,
	}
	var identifiers struct {
		AggregateID string `json:"aggregate_id"`
		EventName   string `json:"event_name"`
	}
	_ = json.Unmarshal(record.Payload, &identifiers)
	entry.AggregateID, entry.EventName = identifiers.AggregateID, identifiers.EventName
	add := func(e activity.Entry) {
		if w.Log != nil {
			w.Log.Add(e)
		}
	}
	return completeRecord(ctx, time.Second, recordActions{
		skipped: func(ctx context.Context) (bool, error) {
			return w.Store.HasSkip(ctx, w.Scope, w.Destination, record)
		},
		send: func(ctx context.Context) (Outcome, error) {
			e := entry
			e.Message = "delivery started"
			add(e)
			outcome, err := w.Processor.Process(ctx, record, func(attempt int, attemptErr error) {
				e := entry
				e.Attempt = attempt + 1
				e.Message = "acknowledged"
				if attemptErr != nil {
					e.Message = "retry"
					e.Level = "warn"
					e.Error = "delivery failed or retry requested"
				}
				add(e)
			})
			if err == nil {
				e := entry
				e.Message = "terminal outcome"
				switch outcome {
				case Delivered:
					e.Outcome = "delivered"
				case Filtered:
					e.Outcome = "filtered"
				case Skipped:
					e.Outcome = "skipped"
				}
				add(e)
			}
			return outcome, err
		},
		audit: func(ctx context.Context) error { return w.Store.SaveSkip(ctx, w.Scope, w.Destination, record) },
		commit: func(ctx context.Context) error {
			if err := commit(ctx, raw); err != nil {
				e := entry
				if ctx.Err() == nil {
					e.Level = "warn"
					e.Message = "commit retry"
					e.Error = "Kafka commit unavailable"
					add(e)
				}
				return err
			}
			e := entry
			e.Message = "committed"
			add(e)
			return nil
		},
	})
}
