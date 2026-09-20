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

func TestProcessorFiltersAndMarshalsOnce(t *testing.T) {
	sender := &fakeSender{outcome: Delivered}
	p := NewProcessor(Destination{ID: "d", Description: "destination", Filter: &protocol.Filter{Column: "kind", Values: []string{"ok"}}}, sender)
	p.Sleep = func(context.Context, time.Duration) error { t.Fatal("filtered record slept"); return nil }
	outcome, err := p.Process(context.Background(), stream.Record{Source: stream.Source{ID: "s", Description: "source"}, Payload: []byte(`{"kind":"no"}`)}, nil)
	if err != nil || outcome != Filtered || sender.calls != 0 {
		t.Fatalf("got %v %v calls=%d", outcome, err, sender.calls)
	}
	outcome, err = p.Process(context.Background(), stream.Record{Source: stream.Source{ID: "s", Description: "source"}, Payload: []byte(`{"kind":"ok"}`)}, nil)
	if err != nil || outcome != Delivered || sender.calls != 1 {
		t.Fatalf("got %v %v calls=%d", outcome, err, sender.calls)
	}
	var envelope protocol.Envelope
	if err := json.Unmarshal(sender.bodies[0], &envelope); err != nil || string(envelope.Payload) != `{"kind":"ok"}` {
		t.Fatalf("envelope=%s err=%v", sender.bodies[0], err)
	}
}

func TestProcessorRetriesAndCallsPerAttempt(t *testing.T) {
	sender := &fakeSender{outcome: Delivered, errs: []error{errors.New("temporary"), nil}}
	p := NewProcessor(Destination{ID: "d"}, sender)
	p.Sleep = func(context.Context, time.Duration) error { return nil }
	p.Jitter = func(int64) int64 { return 0 }
	var attempts []int
	outcome, err := p.Process(context.Background(), stream.Record{Payload: []byte(`{}`)}, func(attempt int, err error) {
		attempts = append(attempts, attempt)
		if attempt == 0 && err == nil {
			t.Fatal("first attempt unexpectedly succeeded")
		}
	})
	if err != nil || outcome != Delivered || sender.calls != 2 {
		t.Fatalf("got %v %v calls=%d", outcome, err, sender.calls)
	}
	if len(attempts) != 2 || attempts[0] != 0 || attempts[1] != 1 {
		t.Fatalf("attempts=%v", attempts)
	}
}
