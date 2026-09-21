package bdd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rafaeelricco/postie/internal/adapters/httpdelivery"
	"github.com/rafaeelricco/postie/internal/stream"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/config"
	"github.com/rafaeelricco/postie/internal/delivery"
	"github.com/rafaeelricco/postie/internal/protocol"
)

// scriptedResponse is one canned HTTP response the fake receiver plays back,
// in order, for successive requests. The last entry repeats once exhausted,
// so "the receiver always answers 500" is just a one-entry script.
type scriptedResponse struct {
	status int
	body   string
}

// recordedRequest captures what the Sender actually sent, for the Then
// steps to assert against.
type recordedRequest struct {
	method      string
	contentType string
	username    string
	password    string
	basicOK     bool
	headers     http.Header
	body        []byte
}

// deliveryWorld is built per scenario by newDeliveryWorld, so scenarios never
// leak state into one another. Its Sender's Sleep records durations instead of
// ever really waiting.
type deliveryWorld struct {
	t      *testing.T
	ctx    context.Context
	cancel context.CancelFunc

	destination config.Destination
	source      stream.Source
	record      stream.Record

	receiver *httptest.Server
	script   []scriptedResponse
	requests []recordedRequest

	sender           *delivery.Processor
	sleeps           []time.Duration
	deliveredPayload json.RawMessage

	outcome delivery.Outcome
	err     error
}

// newDeliveryWorld starts every scenario with destination "projection"
// (Basic credentials user/pass) and source "events".
func newDeliveryWorld(t *testing.T) *deliveryWorld {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	w := &deliveryWorld{
		t:           t,
		ctx:         ctx,
		cancel:      cancel,
		destination: config.Destination{ID: "projection", Description: "projection", Username: "user", Password: "pass"},
		source:      stream.Source{ID: "events", Description: "events"},
	}
	w.sender = delivery.NewProcessor(delivery.Destination{}, nil)
	w.sender.Sleep = func(ctx context.Context, d time.Duration) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		w.sleeps = append(w.sleeps, d)
		return nil
	}
	t.Cleanup(func() {
		if w.receiver != nil {
			w.receiver.Close()
		}
		cancel()
	})
	return w
}

const (
	createdPayload = `{"event_name":"Created"}`
	successAck     = `{"result":{"success":{}}}`
)

// docs/specification.md, "HTTP delivery": request shape, acknowledgements, retries.
// "Delivery and operations": backoff bounds.
func TestDelivery(t *testing.T) {
	t.Run("a success acknowledgement delivers the record", func(t *testing.T) {
		w := newDeliveryWorld(t)
		w.receiverAnswers(200, successAck)
		w.deliverPayload(createdPayload)
		w.outcomeAfterAttempts(delivery.Delivered, 1)
		w.requestWasBasicJSONPost()
		w.requestBodyMatchesContractEnvelope()
	})

	t.Run("anything short of a terminal acknowledgement is retried", func(t *testing.T) {
		cases := []struct {
			name   string
			status int
			body   string
		}{
			{"500", 500, ""},
			{"401 with a success body", 401, successAck},
			{"200 with an empty body", 200, ""},
			{"200 with malformed JSON", 200, `{not json`},
			{"200 with an empty result", 200, `{"result":{}}`},
			{"200 with a null success", 200, `{"result":{"success":null}}`},
			{"200 with must_retry", 200, `{"result":{"error":{"policy":"must_retry","class":"c","description":"d"}}}`},
			{"200 with keep_going missing class", 200, `{"result":{"error":{"policy":"keep_going"}}}`},
			{"200 with an unknown policy", 200, `{"result":{"error":{"policy":"surprise","class":"c","description":"d"}}}`},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				w := newDeliveryWorld(t)
				w.receiverAnswers(c.status, c.body)
				w.receiverAnswersSuccess()
				w.deliverPayload(createdPayload)
				w.outcomeAfterAttempts(delivery.Delivered, 2)
			})
		}
	})

	t.Run("success wins when both outcomes are present", func(t *testing.T) {
		w := newDeliveryWorld(t)
		w.receiverAnswers(200, `{"result":{"success":{},"error":{"policy":"must_retry","class":"c","description":"d"}}}`)
		w.deliverPayload(createdPayload)
		w.outcomeAfterAttempts(delivery.Delivered, 1)
	})

	t.Run("keep_going is a terminal skip", func(t *testing.T) {
		w := newDeliveryWorld(t)
		w.receiverAnswers(200, `{"result":{"error":{"policy":"keep_going","class":"c","description":"d"}}}`)
		w.deliverPayload(createdPayload)
		w.outcomeAfterAttempts(delivery.Skipped, 1)
	})

	t.Run("a response larger than 64 KiB is retried", func(t *testing.T) {
		w := newDeliveryWorld(t)
		w.receiverFirstAnswersOversize()
		w.receiverAnswersSuccess()
		w.deliverPayload(createdPayload)
		w.outcomeAfterAttempts(delivery.Delivered, 2)
	})

	t.Run("every retry wait is between 1 and 60 seconds", func(t *testing.T) {
		w := newDeliveryWorld(t)
		w.receiverFailsThenSucceeds(12)
		w.deliverPayload(createdPayload)
		w.outcomeAfterAttempts(delivery.Delivered, 13)
		w.everySleepBetween1And60()
	})

	t.Run("cancelling stops the retry loop without an outcome", func(t *testing.T) {
		w := newDeliveryWorld(t)
		w.receiverAnswers(500, "") // the last scripted response repeats, so this is "always 500"
		w.nextSleepCancels()
		w.deliverPayload(createdPayload)
		w.deliveryFailedWithCancellation()
		w.outcomeIsZeroValue()
		w.receiverRequestCountIs(1)
	})

	t.Run("diagnostic headers identify the record", func(t *testing.T) {
		w := newDeliveryWorld(t)
		w.record = stream.Record{
			Source:     w.source,
			Payload:    json.RawMessage(`{}`),
			Topic:      "events",
			Partition:  3,
			Offset:     42,
			EventID:    "evt-1",
			Generation: 2,
			Replay:     true,
		}
		w.receiverAnswersSuccess()
		w.deliverRecord()
		w.outcomeAfterAttempts(delivery.Delivered, 1)
		w.requestCarriedDiagnosticHeaders()
	})
}

// docs/specification.md, "HTTP delivery": filters.
func TestFilter(t *testing.T) {
	created := &config.Filter{Column: "event_name", Values: []string{"Created"}}
	cases := []struct {
		name     string
		filter   *config.Filter
		payload  string
		outcome  delivery.Outcome
		requests int
	}{
		{"matching value", created, `{"event_name":"Created"}`, delivery.Delivered, 1},
		{"other value", created, `{"event_name":"Deleted"}`, delivery.Filtered, 0},
		{"column absent", created, `{"other":1}`, delivery.Delivered, 1},
		{"non-string column", created, `{"event_name":3}`, delivery.Delivered, 1},
		{"non-object payload", created, `"just text"`, delivery.Delivered, 1},
		{"no filter", nil, `{"event_name":"Deleted"}`, delivery.Delivered, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newDeliveryWorld(t)
			w.receiverAnswersSuccess()
			w.destination.Filter = c.filter
			w.deliverPayload(c.payload)
			w.outcomeIs(c.outcome)
			w.receiverRequestCountIs(c.requests)
		})
	}
}

func (w *deliveryWorld) ensureReceiver() {
	if w.receiver != nil {
		return
	}
	w.receiver = httptest.NewServer(http.HandlerFunc(w.serve))
	w.destination.Endpoint = w.receiver.URL
}

func (w *deliveryWorld) serve(rw http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	user, pass, ok := r.BasicAuth()
	w.requests = append(w.requests, recordedRequest{
		method:      r.Method,
		contentType: r.Header.Get("Content-Type"),
		username:    user,
		password:    pass,
		basicOK:     ok,
		headers:     r.Header.Clone(),
		body:        body,
	})
	idx := len(w.requests) - 1
	if idx >= len(w.script) {
		idx = len(w.script) - 1
	}
	if idx < 0 {
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	resp := w.script[idx]
	rw.WriteHeader(resp.status)
	_, _ = rw.Write([]byte(resp.body))
}

func (w *deliveryWorld) receiverAnswers(status int, body string) {
	w.ensureReceiver()
	w.script = append(w.script, scriptedResponse{status: status, body: body})
}

func (w *deliveryWorld) receiverAnswersSuccess() { w.receiverAnswers(200, successAck) }

func (w *deliveryWorld) receiverFirstAnswersOversize() {
	w.ensureReceiver()
	big := fmt.Sprintf(`{"result":{"success":{"pad":"%s"}}}`, strings.Repeat("x", 70*1024))
	w.script = append(w.script, scriptedResponse{status: 200, body: big})
}

func (w *deliveryWorld) receiverFailsThenSucceeds(times int) {
	w.ensureReceiver()
	for i := 0; i < times; i++ {
		w.script = append(w.script, scriptedResponse{status: 500, body: ""})
	}
	w.script = append(w.script, scriptedResponse{status: 200, body: successAck})
}

func (w *deliveryWorld) nextSleepCancels() {
	w.sender.Sleep = func(ctx context.Context, _ time.Duration) error {
		w.cancel()
		return ctx.Err()
	}
}

func (w *deliveryWorld) deliverPayload(payload string) {
	w.record.Source = w.source
	w.record.Payload = json.RawMessage(payload)
	w.deliveredPayload = json.RawMessage(payload)
	w.bindDestination()
	w.outcome, w.err = w.sender.Process(w.ctx, w.record, nil)
}

func (w *deliveryWorld) deliverRecord() {
	w.deliveredPayload = w.record.Payload
	w.bindDestination()
	w.outcome, w.err = w.sender.Process(w.ctx, w.record, nil)
}

func (w *deliveryWorld) outcomeAfterAttempts(want delivery.Outcome, attempts int) {
	w.t.Helper()
	w.outcomeIs(want)
	if got := len(w.requests); got != attempts {
		w.t.Fatalf("expected %d attempts, got %d", attempts, got)
	}
}

func (w *deliveryWorld) outcomeIs(want delivery.Outcome) {
	w.t.Helper()
	if w.err != nil {
		w.t.Fatalf("expected outcome %v, got error: %v", want, w.err)
	}
	if w.outcome != want {
		w.t.Fatalf("expected outcome %v, got %v", want, w.outcome)
	}
}

func (w *deliveryWorld) requestWasBasicJSONPost() {
	w.t.Helper()
	if len(w.requests) == 0 {
		w.t.Fatalf("no request recorded")
	}
	r := w.requests[len(w.requests)-1]
	if r.method != http.MethodPost {
		w.t.Fatalf("expected POST, got %s", r.method)
	}
	if r.contentType != "application/json" {
		w.t.Fatalf("expected Content-Type application/json, got %q", r.contentType)
	}
	if !r.basicOK || r.username != w.destination.Username || r.password != w.destination.Password {
		w.t.Fatalf("expected Basic auth %s:%s, got ok=%v %s:%s", w.destination.Username, w.destination.Password, r.basicOK, r.username, r.password)
	}
}

func (w *deliveryWorld) requestBodyMatchesContractEnvelope() {
	w.t.Helper()
	if len(w.requests) == 0 {
		w.t.Fatalf("no request recorded")
	}
	want, err := json.Marshal(protocol.Envelope{
		DataSourceID:               w.source.ID,
		DataSourceDescription:      w.source.Description,
		DataDestinationID:          w.destination.ID,
		DataDestinationDescription: w.destination.Description,
		Payload:                    w.deliveredPayload,
	})
	if err != nil {
		w.t.Fatal(err)
	}
	got := w.requests[len(w.requests)-1].body
	if string(got) != string(want) {
		w.t.Fatalf("request body = %s, want %s", got, want)
	}
}

func (w *deliveryWorld) everySleepBetween1And60() {
	w.t.Helper()
	if len(w.sleeps) == 0 {
		w.t.Fatalf("no sleeps recorded")
	}
	for _, d := range w.sleeps {
		if d < time.Second || d > 60*time.Second {
			w.t.Fatalf("sleep %v out of bounds [1s,60s]", d)
		}
	}
}

func (w *deliveryWorld) deliveryFailedWithCancellation() {
	w.t.Helper()
	if !errors.Is(w.err, context.Canceled) {
		w.t.Fatalf("expected context.Canceled, got %v", w.err)
	}
}

func (w *deliveryWorld) outcomeIsZeroValue() {
	w.t.Helper()
	if w.outcome != 0 {
		w.t.Fatalf("expected zero-value outcome, got %v", w.outcome)
	}
}

func (w *deliveryWorld) requestCarriedDiagnosticHeaders() {
	w.t.Helper()
	if len(w.requests) == 0 {
		w.t.Fatalf("no request recorded")
	}
	got := w.requests[len(w.requests)-1].headers
	checks := map[string]string{
		"X-Postie-Event-ID":            w.record.EventID,
		"X-Postie-Delivery-Generation": w.record.Generation.String(),
		"X-Postie-Replay":              fmt.Sprint(w.record.Replay),
		"X-Postie-Topic":               w.record.Topic,
		"X-Postie-Partition":           fmt.Sprint(w.record.Partition),
		"X-Postie-Offset":              fmt.Sprint(w.record.Offset),
		"Idempotency-Key":              w.record.EventID + ":" + w.destination.ID + ":" + w.record.Generation.String(),
	}
	for header, want := range checks {
		if got := got.Get(header); got != want {
			w.t.Fatalf("%s = %q, want %q", header, got, want)
		}
	}
}

func (w *deliveryWorld) receiverRequestCountIs(n int) {
	w.t.Helper()
	if got := len(w.requests); got != n {
		w.t.Fatalf("expected %d requests, got %d", n, got)
	}
}

func (w *deliveryWorld) bindDestination() {
	var filter *protocol.Filter
	if w.destination.Filter != nil {
		filter = &protocol.Filter{Column: w.destination.Filter.Column, Values: w.destination.Filter.Values}
	}
	w.sender.Destination = delivery.Destination{ID: w.destination.ID, Description: w.destination.Description, Filter: filter}
	w.sender.Sender = httpdelivery.NewClient(nil, httpdelivery.Destination{ID: w.destination.ID, Endpoint: w.destination.Endpoint, Username: w.destination.Username, Password: w.destination.Password})
}
