package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

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
