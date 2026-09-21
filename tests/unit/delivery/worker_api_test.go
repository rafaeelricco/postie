package delivery_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/delivery"
	"github.com/rafaeelricco/postie/internal/protocol"
	"github.com/rafaeelricco/postie/internal/stream"
)

type apiSkipStore struct {
	has       bool
	hasCalls  int
	saveCalls int
}

func (s *apiSkipStore) HasSkip(context.Context, stream.Scope, string, stream.Record) (bool, error) {
	s.hasCalls++
	return s.has, nil
}
func (s *apiSkipStore) SaveSkip(context.Context, stream.Scope, string, stream.Record) error {
	s.saveCalls++
	return nil
}

type apiGate struct {
	allowed bool
	blocks  []struct{ source, reason string }
}

func (g *apiGate) Allowed(string) bool { return g.allowed }
func (g *apiGate) Block(source, reason string) {
	g.blocks = append(g.blocks, struct{ source, reason string }{source, reason})
}

type apiSender struct {
	outcome delivery.Outcome
	err     error
	calls   int
}

func (s *apiSender) Send(context.Context, stream.Record, []byte) (delivery.Outcome, error) {
	s.calls++
	return s.outcome, s.err
}

func apiRecord() stream.Record {
	return stream.Record{
		Source:  stream.Source{ID: "events", Description: "Event stream"},
		Payload: []byte(`{"aggregate_id":"agg-1","event_name":"Created"}`),
		Topic:   "events.topic", Partition: 2, Offset: 9, EventID: "event-9", Generation: 3,
	}
}

func newAPIWorker(record stream.Record, store *apiSkipStore, gate *apiGate, sender *apiSender, log *activity.Log) *delivery.Worker {
	processor := delivery.NewProcessor(delivery.Destination{ID: "projection", Description: "Projection"}, sender)
	processor.Sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	return &delivery.Worker{
		Processor: processor,
		Decode:    func(*stream.RawRecord) (stream.Record, error) { return record, nil },
		Store:     store, Scope: stream.Scope{Namespace: "n", Environment: "e", Generation: 3},
		Destination: "projection", SourceID: "events", Gate: gate, Log: log,
	}
}

func apiEntries(t *testing.T, log *activity.Log) []activity.Entry {
	t.Helper()
	page, err := log.Read("")
	if err != nil {
		t.Fatal(err)
	}
	return page.Entries
}

func TestWorkerProcessDecodeErrorDurablyBlocksBoundSource(t *testing.T) {
	gate := &apiGate{allowed: true}
	w := &delivery.Worker{
		Decode: func(*stream.RawRecord) (stream.Record, error) {
			return stream.Record{}, errors.New("payload is malformed")
		},
		SourceID: "bound-source", Gate: gate, Store: &apiSkipStore{}, Processor: &delivery.Processor{},
	}
	err := w.Process(context.Background(), &stream.RawRecord{}, func(context.Context, *stream.RawRecord) error { t.Fatal("committed decode failure"); return nil })
	if err == nil || err.Error() != "payload is malformed" {
		t.Fatalf("error=%v", err)
	}
	if len(gate.blocks) != 1 || gate.blocks[0].source != "bound-source" || gate.blocks[0].reason != "payload is malformed" {
		t.Fatalf("blocks=%+v", gate.blocks)
	}
}

func TestWorkerProcessExistingDurableSkipOnlyCommits(t *testing.T) {
	store := &apiSkipStore{has: true}
	sender := &apiSender{outcome: delivery.Delivered}
	gate := &apiGate{allowed: true}
	w := newAPIWorker(apiRecord(), store, gate, sender, activity.New(io.Discard))
	commits := 0
	err := w.Process(context.Background(), &stream.RawRecord{}, func(context.Context, *stream.RawRecord) error { commits++; return nil })
	if err != nil || commits != 1 || sender.calls != 0 || store.saveCalls != 0 {
		t.Fatalf("err=%v commits=%d sends=%d saves=%d", err, commits, sender.calls, store.saveCalls)
	}
	entries := apiEntries(t, w.Log)
	if len(entries) != 1 || entries[0].Message != "committed" {
		t.Fatalf("entries=%+v", entries)
	}
}

func TestWorkerProcessDeliveredLogsMetadataAndCommits(t *testing.T) {
	store := &apiSkipStore{}
	sender := &apiSender{outcome: delivery.Delivered}
	log := activity.New(io.Discard)
	w := newAPIWorker(apiRecord(), store, &apiGate{allowed: true}, sender, log)
	commits := 0
	if err := w.Process(context.Background(), &stream.RawRecord{}, func(context.Context, *stream.RawRecord) error { commits++; return nil }); err != nil {
		t.Fatal(err)
	}
	if commits != 1 || sender.calls != 1 || store.saveCalls != 0 {
		t.Fatalf("commits=%d sends=%d saves=%d", commits, sender.calls, store.saveCalls)
	}
	entries := apiEntries(t, log)
	if len(entries) != 4 {
		t.Fatalf("entries=%d: %+v", len(entries), entries)
	}
	if entries[0].Message != "delivery started" || entries[1].Message != "acknowledged" || entries[2].Message != "terminal outcome" || entries[2].Outcome != "delivered" || entries[3].Message != "committed" {
		t.Fatalf("entries=%+v", entries)
	}
	for _, entry := range entries {
		if entry.Source != "events" || entry.Destination != "projection" || entry.Generation != 3 || entry.Topic != "events.topic" || entry.Partition != 2 || entry.Offset != 9 || entry.EventID != "event-9" || entry.AggregateID != "agg-1" || entry.EventName != "Created" {
			t.Fatalf("metadata=%+v", entry)
		}
	}
}

func TestWorkerProcessFilteredCompletesWithoutSend(t *testing.T) {
	store := &apiSkipStore{}
	sender := &apiSender{outcome: delivery.Delivered}
	log := activity.New(io.Discard)
	w := newAPIWorker(apiRecord(), store, &apiGate{allowed: true}, sender, log)
	w.Processor.Destination.Filter = &protocol.Filter{Column: "event_name", Values: []string{"Deleted"}}
	commits := 0
	if err := w.Process(context.Background(), &stream.RawRecord{}, func(context.Context, *stream.RawRecord) error { commits++; return nil }); err != nil {
		t.Fatal(err)
	}
	if commits != 1 || sender.calls != 0 || store.saveCalls != 0 {
		t.Fatalf("commits=%d sends=%d saves=%d", commits, sender.calls, store.saveCalls)
	}
	entries := apiEntries(t, log)
	if len(entries) != 3 || entries[1].Outcome != "filtered" || entries[2].Message != "committed" {
		t.Fatalf("entries=%+v", entries)
	}
}

func TestWorkerProcessSkipAuditsBeforeCommit(t *testing.T) {
	store := &apiSkipStore{}
	sender := &apiSender{outcome: delivery.Skipped}
	log := activity.New(io.Discard)
	w := newAPIWorker(apiRecord(), store, &apiGate{allowed: true}, sender, log)
	commits := 0
	if err := w.Process(context.Background(), &stream.RawRecord{}, func(context.Context, *stream.RawRecord) error {
		if store.saveCalls == 0 {
			t.Fatal("commit before durable skip audit")
		}
		commits++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if commits != 1 || sender.calls != 1 || store.saveCalls != 1 {
		t.Fatalf("commits=%d sends=%d saves=%d", commits, sender.calls, store.saveCalls)
	}
	entries := apiEntries(t, log)
	if len(entries) != 4 || entries[2].Outcome != "skipped" || entries[3].Message != "committed" {
		t.Fatalf("entries=%+v", entries)
	}
}

func TestWorkerProcessCancellationStopsBeforeCommit(t *testing.T) {
	store := &apiSkipStore{}
	sender := &apiSender{err: context.Canceled}
	w := newAPIWorker(apiRecord(), store, &apiGate{allowed: true}, sender, activity.New(io.Discard))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Processor.Sleep = func(context.Context, time.Duration) error { cancel(); return context.Canceled }
	err := w.Process(ctx, &stream.RawRecord{}, func(context.Context, *stream.RawRecord) error { t.Fatal("committed cancelled delivery"); return nil })
	if !errors.Is(err, context.Canceled) || sender.calls != 1 || store.saveCalls != 0 {
		t.Fatalf("err=%v sends=%d saves=%d", err, sender.calls, store.saveCalls)
	}
}

func TestWorkerProcessDisallowedGateStopsBeforeDecode(t *testing.T) {
	decoded := false
	gate := &apiGate{allowed: false}
	w := &delivery.Worker{SourceID: "events", Gate: gate, Decode: func(*stream.RawRecord) (stream.Record, error) { decoded = true; return stream.Record{}, nil }}
	err := w.Process(context.Background(), &stream.RawRecord{}, nil)
	if !errors.Is(err, context.Canceled) || decoded {
		t.Fatalf("err=%v decoded=%v", err, decoded)
	}
}
