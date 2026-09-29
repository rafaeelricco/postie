package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/protocol"
	"github.com/rafaeelricco/postie/internal/stream"
)

type fakeSender struct {
	calls   int
	bodies  [][]byte
	errs    []error
	outcome Outcome
}

func (s *fakeSender) Send(_ context.Context, _ stream.Record, body []byte) (Outcome, error) {
	s.calls++
	s.bodies = append(s.bodies, append([]byte(nil), body...))
	if len(s.errs) > 0 {
		err := s.errs[0]
		s.errs = s.errs[1:]
		if err != nil {
			return 0, err
		}
	}
	return s.outcome, nil
}

func TestProcessSendsEnvelope(t *testing.T) {
	sender := &fakeSender{outcome: Delivered}
	p := NewProcessor(Destination{ID: "projection", Description: "projection"}, sender)
	record := stream.Record{Source: stream.Source{ID: "events", Description: "events"}, Payload: []byte(`{"event_name":"Created"}`), Topic: "events", Generation: 1}
	outcome, err := p.Process(context.Background(), record, nil)
	if err != nil || outcome != Delivered {
		t.Fatalf("got %v %v", outcome, err)
	}
	if string(sender.bodies[0]) != `{"data_source_id":"events","data_source_description":"events","data_destination_id":"projection","data_destination_description":"projection","payload":{"event_name":"Created"}}` {
		t.Fatalf("unexpected body %s", sender.bodies[0])
	}
	var envelope protocol.Envelope
	if err := json.Unmarshal(sender.bodies[0], &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.DataSourceID != "events" || envelope.DataSourceDescription != "events" || envelope.DataDestinationID != "projection" || envelope.DataDestinationDescription != "projection" || string(envelope.Payload) != string(record.Payload) {
		t.Fatalf("unexpected envelope: %s", sender.bodies[0])
	}
}

func TestProcessFilteredSendsNothing(t *testing.T) {
	sender := &fakeSender{outcome: Delivered}
	p := NewProcessor(Destination{ID: "d", Filter: &protocol.Filter{Column: "event_name", Values: []string{"Created"}}}, sender)
	p.Sleep = func(context.Context, time.Duration) error { t.Fatal("filtered record slept"); return nil }
	outcome, err := p.Process(context.Background(), stream.Record{Payload: []byte(`{"event_name":"Deleted"}`)}, nil)
	if err != nil || outcome != Filtered || sender.calls != 0 {
		t.Fatalf("got %v %v calls=%d", outcome, err, sender.calls)
	}
	outcome, err = p.Process(context.Background(), stream.Record{Payload: []byte(`{"event_name":"Created"}`)}, nil)
	if err != nil || outcome != Delivered || sender.calls != 1 {
		t.Fatalf("matching record: got %v %v calls=%d", outcome, err, sender.calls)
	}
}

func TestOnAttemptSeesEveryAttempt(t *testing.T) {
	sender := &fakeSender{outcome: Delivered, errs: []error{errors.New("first"), errors.New("second"), nil}}
	p := NewProcessor(Destination{ID: "d"}, sender)
	p.Sleep = func(context.Context, time.Duration) error { return nil }
	var attempts []struct {
		n      int
		failed bool
	}
	outcome, err := p.Process(context.Background(), stream.Record{Payload: []byte(`{}`)}, func(n int, err error) {
		attempts = append(attempts, struct {
			n      int
			failed bool
		}{n, err != nil})
	})
	if err != nil || outcome != Delivered {
		t.Fatalf("got %v %v", outcome, err)
	}
	want := []struct {
		n      int
		failed bool
	}{{0, true}, {1, true}, {2, false}}
	if len(attempts) != len(want) {
		t.Fatalf("attempts=%v", attempts)
	}
	for i := range want {
		if attempts[i] != want[i] {
			t.Fatalf("attempt %d=%v want %v", i, attempts[i], want[i])
		}
	}
}

func TestCtxSleep(t *testing.T) {
	if err := ctxSleep(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := ctxSleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	} else if time.Since(start) > 100*time.Millisecond {
		t.Fatal("cancelled sleep did not return promptly")
	}
}

func TestNewSenderDefaults(t *testing.T) {
	p := NewProcessor(Destination{}, &fakeSender{})
	if p.Sleep == nil || p.Jitter == nil {
		t.Fatalf("defaults missing: %+v", p)
	}
	if p.Sender == nil {
		t.Fatal("sender missing")
	}
}

func TestBackoffBounds(t *testing.T) {
	floor := func(int64) int64 { return 0 }
	top := func(n int64) int64 { return n - 1 }
	for attempt := 0; attempt <= 10; attempt++ {
		for _, jitter := range []func(int64) int64{floor, top} {
			delay := backoff(attempt, jitter)
			if delay < time.Second || delay > 60*time.Second {
				t.Fatalf("attempt %d: %v", attempt, delay)
			}
		}
	}
}

func TestBackoffExactValues(t *testing.T) {
	floor := func(int64) int64 { return 0 }
	top := func(n int64) int64 { return n - 1 }
	for _, tc := range []struct {
		attempt int
		jitter  func(int64) int64
		want    time.Duration
	}{{0, floor, time.Second}, {1, floor, time.Second}, {2, floor, 2 * time.Second}, {2, top, 4 * time.Second}, {5, floor, 16 * time.Second}, {5, top, 32 * time.Second}, {6, floor, 32 * time.Second}, {6, top, 60 * time.Second}, {40, top, 60 * time.Second}} {
		if got := backoff(tc.attempt, tc.jitter); got != tc.want {
			t.Fatalf("backoff(%d)=%v want %v", tc.attempt, got, tc.want)
		}
	}
	var asked int64
	backoff(2, func(n int64) int64 { asked = n; return 0 })
	if asked != int64(2*time.Second)+1 {
		t.Fatalf("jitter range=%d", asked)
	}
}

func TestOutcomeZeroValueIsInvalid(t *testing.T) {
	var outcome Outcome
	if outcome == Delivered || outcome == Filtered || outcome == Skipped {
		t.Fatalf("zero value %v is valid", outcome)
	}
}

func TestTerminalOutcomeBeforeCommit(t *testing.T) {
	for _, outcome := range []Outcome{Delivered, Filtered, Skipped} {
		t.Run(string(rune('0'+outcome)), func(t *testing.T) {
			processed, audited, committed := 0, 0, 0
			err := completeRecord(context.Background(), time.Millisecond, recordActions{
				skipped: func(context.Context) (bool, error) { return false, nil },
				send:    func(context.Context) (Outcome, error) { processed++; return outcome, nil },
				audit: func(context.Context) error {
					audited++
					if committed != 0 {
						t.Fatal("commit before audit")
					}
					if audited == 1 {
						return errors.New("store temporarily down")
					}
					return nil
				},
				commit: func(context.Context) error {
					committed++
					if outcome == Skipped && audited < 2 {
						t.Fatal("missing durable audit")
					}
					if committed == 1 {
						return errors.New("broker temporarily down")
					}
					return nil
				},
			})
			if err != nil || processed != 1 || committed != 2 {
				t.Fatalf("process=%d commit=%d err=%v", processed, committed, err)
			}
			if outcome == Skipped && audited != 2 {
				t.Fatal("audit was not retried")
			}
			if outcome != Skipped && audited != 0 {
				t.Fatal("non-skip audited")
			}
		})
	}
}
func TestPreviouslyAuditedSkipOnlyCommits(t *testing.T) {
	commits := 0
	err := completeRecord(context.Background(), time.Millisecond, recordActions{
		skipped: func(context.Context) (bool, error) { return true, nil },
		send:    func(context.Context) (Outcome, error) { t.Fatal("skip was redelivered"); return 0, nil },
		audit:   func(context.Context) error { t.Fatal("skip was re-audited"); return nil },
		commit:  func(context.Context) error { commits++; return nil },
	})
	if err != nil || commits != 1 {
		t.Fatalf("commits=%d err=%v", commits, err)
	}
}
func TestCancellationNeverCommitsIncompleteRecord(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	err := completeRecord(ctx, time.Millisecond, recordActions{
		skipped: func(context.Context) (bool, error) { return false, nil },
		send:    func(context.Context) (Outcome, error) { cancel(); return 0, context.Canceled },
		commit:  func(context.Context) error { t.Fatal("committed retrying record"); return nil },
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	checks := 0
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = completeRecord(ctx, time.Millisecond, recordActions{
		skipped: func(context.Context) (bool, error) { checks++; return false, errors.New("store down") },
		send:    func(context.Context) (Outcome, error) { t.Fatal("sent without skip check"); return 0, nil },
	})
	if !errors.Is(err, context.DeadlineExceeded) || checks < 2 {
		t.Fatalf("checks=%d err=%v", checks, err)
	}
}

type skipStore struct {
	has   bool
	saves int
}

func (s *skipStore) HasSkip(context.Context, stream.Scope, string, stream.Record) (bool, error) {
	return s.has, nil
}
func (s *skipStore) SaveSkip(context.Context, stream.Scope, string, stream.Record) error {
	s.saves++
	return nil
}

func TestWorkerPreviouslyAuditedSkipOnlyCommits(t *testing.T) {
	store := &skipStore{has: true}
	worker := Worker{Store: store, Scope: stream.Scope{Generation: 1}, Destination: "d", Decode: func(*stream.RawRecord) (stream.Record, error) { return stream.Record{}, nil }, Processor: &Processor{}}
	commits := 0
	err := worker.Process(context.Background(), &stream.RawRecord{Historical: true}, func(context.Context, *stream.RawRecord) error { commits++; return nil })
	if err != nil || commits != 1 || store.saves != 0 {
		t.Fatalf("err=%v commits=%d saves=%d", err, commits, store.saves)
	}
}

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

func apiRecord() stream.Record {
	return stream.Record{
		Source:  stream.Source{ID: "events", Description: "Event stream"},
		Payload: []byte(`{"aggregate_id":"agg-1","event_name":"Created"}`),
		Topic:   "events.topic", Partition: 2, Offset: 9, EventID: "event-9", Generation: 3,
	}
}

func newAPIWorker(record stream.Record, store *apiSkipStore, gate *apiGate, sender *fakeSender, log *activity.Log) *Worker {
	processor := NewProcessor(Destination{ID: "projection", Description: "Projection"}, sender)
	processor.Sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	return &Worker{
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
	w := &Worker{
		Decode: func(*stream.RawRecord) (stream.Record, error) {
			return stream.Record{}, errors.New("payload is malformed")
		},
		SourceID: "bound-source", Gate: gate, Store: &apiSkipStore{}, Processor: &Processor{},
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
	sender := &fakeSender{outcome: Delivered}
	gate := &apiGate{allowed: true}
	w := newAPIWorker(apiRecord(), store, gate, sender, activity.New(io.Discard))
	commits := 0
	err := w.Process(context.Background(), &stream.RawRecord{Historical: true}, func(context.Context, *stream.RawRecord) error { commits++; return nil })
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
	sender := &fakeSender{outcome: Delivered}
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
	sender := &fakeSender{outcome: Delivered}
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
	sender := &fakeSender{outcome: Skipped}
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
	sender := &fakeSender{errs: []error{context.Canceled}}
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
	w := &Worker{SourceID: "events", Gate: gate, Decode: func(*stream.RawRecord) (stream.Record, error) { decoded = true; return stream.Record{}, nil }}
	err := w.Process(context.Background(), &stream.RawRecord{}, nil)
	if !errors.Is(err, context.Canceled) || decoded {
		t.Fatalf("err=%v decoded=%v", err, decoded)
	}
}

// A live record is being read for the first time, so it cannot already carry a
// durable skip: the store is never asked, and a stored skip left over from some
// other record must not suppress the send.
func TestWorkerProcessLiveRecordSkipsTheSkipLookup(t *testing.T) {
	store := &apiSkipStore{has: true}
	sender := &fakeSender{outcome: Delivered}
	gate := &apiGate{allowed: true}
	w := newAPIWorker(apiRecord(), store, gate, sender, activity.New(io.Discard))
	commits := 0
	err := w.Process(context.Background(), &stream.RawRecord{}, func(context.Context, *stream.RawRecord) error { commits++; return nil })
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if store.hasCalls != 0 {
		t.Fatalf("live record consulted the skip store: hasCalls=%d", store.hasCalls)
	}
	if sender.calls != 1 {
		t.Fatalf("stored skip suppressed a live record: sends=%d", sender.calls)
	}
	if commits != 1 {
		t.Fatalf("commits=%d", commits)
	}
}

// A re-read record may have been skipped before the crash that lost its offset
// commit, so it must still be checked against the durable skip.
func TestWorkerProcessHistoricalRecordConsultsTheSkipStore(t *testing.T) {
	store := &apiSkipStore{has: true}
	sender := &fakeSender{outcome: Delivered}
	gate := &apiGate{allowed: true}
	w := newAPIWorker(apiRecord(), store, gate, sender, activity.New(io.Discard))
	commits := 0
	err := w.Process(context.Background(), &stream.RawRecord{Historical: true}, func(context.Context, *stream.RawRecord) error { commits++; return nil })
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if store.hasCalls != 1 {
		t.Fatalf("historical record skipped the skip lookup: hasCalls=%d", store.hasCalls)
	}
	if sender.calls != 0 {
		t.Fatalf("stored skip was redelivered: sends=%d", sender.calls)
	}
	if commits != 1 {
		t.Fatalf("commits=%d", commits)
	}
}
