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
	"time"

	"github.com/cucumber/godog"

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

// deliveryWorld is rebuilt from scratch in sc.Before for every scenario, so
// scenarios never leak state into one another. Its Sender's Sleep records
// durations instead of ever really waiting.
type deliveryWorld struct {
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

func registerDeliverySteps(sc *godog.ScenarioContext) {
	w := &deliveryWorld{}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		cctx, cancel := context.WithCancel(context.Background())
		*w = deliveryWorld{ctx: cctx, cancel: cancel}
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
		return ctx, nil
	})
	sc.After(func(ctx context.Context, _ *godog.Scenario, err error) (context.Context, error) {
		if w.receiver != nil {
			w.receiver.Close()
		}
		if w.cancel != nil {
			w.cancel()
		}
		return ctx, err
	})

	sc.Given(`^a destination "([^"]*)" with Basic credentials "([^"]*)" and "([^"]*)"$`, w.aDestination)
	sc.Given(`^a source "([^"]*)"$`, w.aSource)
	sc.Given(`^the receiver answers (\d+) with body:$`, w.receiverAnswersWithBody)
	sc.Given(`^the receiver answers 200 with a success acknowledgement$`, w.receiverAnswersSuccess)
	sc.Given(`^the receiver first answers (\d+) with body '([^']*)'$`, w.receiverFirstAnswers)
	sc.Given(`^the receiver then answers 200 with a success acknowledgement$`, w.receiverAnswersSuccess)
	sc.Given(`^the receiver first answers 200 with a body larger than 64 KiB$`, w.receiverFirstAnswersOversize)
	sc.Given(`^the receiver fails (\d+) times then answers 200 with a success acknowledgement$`, w.receiverFailsThenSucceeds)
	sc.Given(`^the receiver always answers (\d+)$`, w.receiverAlwaysAnswers)
	sc.Given(`^the next retry sleep cancels the delivery$`, w.nextSleepCancels)
	sc.Given(`^a record with topic "([^"]*)", partition (\d+), offset (\d+), event id "([^"]*)", generation (\d+), replay (true|false)$`, w.aRecord)
	sc.Given(`^the destination filter is on column "([^"]*)" with values "([^"]*)"$`, w.destinationFilterOnColumn)

	sc.When(`^the engine delivers the payload '([^']*)'$`, w.deliverPayload)
	sc.When(`^the engine delivers the record$`, w.deliverRecord)

	sc.Then(`^the outcome is "([^"]*)" after (\d+) attempts?$`, w.outcomeAfterAttempts)
	sc.Then(`^the outcome is "([^"]*)"$`, w.outcomeIs)
	sc.Then(`^the request was a Basic-authenticated JSON POST$`, w.requestWasBasicJSONPost)
	sc.Then(`^the request body matches the delivery contract envelope around that payload$`, w.requestBodyMatchesContractEnvelope)
	sc.Then(`^every recorded sleep was between 1 and 60 seconds$`, w.everySleepBetween1And60)
	sc.Then(`^the delivery failed with a cancellation error$`, w.deliveryFailedWithCancellation)
	sc.Then(`^the outcome is the zero value$`, w.outcomeIsZeroValue)
	sc.Then(`^the request carried the diagnostic headers for that record$`, w.requestCarriedDiagnosticHeaders)
	sc.Then(`^the receiver request count is (\d+)$`, w.receiverRequestCountIs)
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

func (w *deliveryWorld) aDestination(id, username, password string) error {
	w.destination = config.Destination{ID: id, Description: id, Username: username, Password: password}
	return nil
}

func (w *deliveryWorld) aSource(id string) error {
	w.source = stream.Source{ID: id, Description: id}
	return nil
}

func (w *deliveryWorld) receiverAnswersWithBody(status int, doc *godog.DocString) error {
	w.ensureReceiver()
	w.script = append(w.script, scriptedResponse{status: status, body: doc.Content})
	return nil
}

func (w *deliveryWorld) receiverAnswersSuccess() error {
	w.ensureReceiver()
	w.script = append(w.script, scriptedResponse{status: 200, body: `{"result":{"success":{}}}`})
	return nil
}

func (w *deliveryWorld) receiverFirstAnswers(status int, body string) error {
	w.ensureReceiver()
	w.script = append(w.script, scriptedResponse{status: status, body: body})
	return nil
}

func (w *deliveryWorld) receiverFirstAnswersOversize() error {
	w.ensureReceiver()
	big := fmt.Sprintf(`{"result":{"success":{"pad":"%s"}}}`, strings.Repeat("x", 70*1024))
	w.script = append(w.script, scriptedResponse{status: 200, body: big})
	return nil
}

func (w *deliveryWorld) receiverFailsThenSucceeds(times int) error {
	w.ensureReceiver()
	for i := 0; i < times; i++ {
		w.script = append(w.script, scriptedResponse{status: 500, body: ""})
	}
	w.script = append(w.script, scriptedResponse{status: 200, body: `{"result":{"success":{}}}`})
	return nil
}

func (w *deliveryWorld) receiverAlwaysAnswers(status int) error {
	w.ensureReceiver()
	w.script = append(w.script, scriptedResponse{status: status, body: ""})
	return nil
}

func (w *deliveryWorld) nextSleepCancels() error {
	w.sender.Sleep = func(ctx context.Context, _ time.Duration) error {
		w.cancel()
		return ctx.Err()
	}
	return nil
}

func (w *deliveryWorld) aRecord(topic string, partition, offset int, eventID string, generation int, replay string) error {
	w.record = stream.Record{
		Source:     w.source,
		Payload:    json.RawMessage(`{}`),
		Topic:      topic,
		Partition:  int32(partition),
		Offset:     int64(offset),
		EventID:    eventID,
		Generation: stream.Generation(generation),
		Replay:     replay == "true",
	}
	return nil
}

func (w *deliveryWorld) destinationFilterOnColumn(column, values string) error {
	if column == "" {
		w.destination.Filter = nil
		return nil
	}
	w.destination.Filter = &config.Filter{Column: column, Values: strings.Split(values, ",")}
	return nil
}

func (w *deliveryWorld) deliverPayload(payload string) error {
	w.record.Source = w.source
	w.record.Payload = json.RawMessage(payload)
	w.deliveredPayload = json.RawMessage(payload)
	w.bindDestination()
	w.outcome, w.err = w.sender.Process(w.ctx, w.record, nil)
	return nil
}

func (w *deliveryWorld) deliverRecord() error {
	w.deliveredPayload = w.record.Payload
	w.bindDestination()
	w.outcome, w.err = w.sender.Process(w.ctx, w.record, nil)
	return nil
}

func outcomeByName(name string) (delivery.Outcome, error) {
	switch name {
	case "delivered":
		return delivery.Delivered, nil
	case "filtered":
		return delivery.Filtered, nil
	case "skipped":
		return delivery.Skipped, nil
	default:
		return 0, fmt.Errorf("unknown outcome name %q", name)
	}
}

func (w *deliveryWorld) outcomeAfterAttempts(name string, attempts int) error {
	if w.err != nil {
		return fmt.Errorf("expected outcome %q, got error: %w", name, w.err)
	}
	want, err := outcomeByName(name)
	if err != nil {
		return err
	}
	if w.outcome != want {
		return fmt.Errorf("expected outcome %v, got %v", want, w.outcome)
	}
	if got := len(w.requests); got != attempts {
		return fmt.Errorf("expected %d attempts, got %d", attempts, got)
	}
	return nil
}

func (w *deliveryWorld) outcomeIs(name string) error {
	if w.err != nil {
		return fmt.Errorf("expected outcome %q, got error: %w", name, w.err)
	}
	want, err := outcomeByName(name)
	if err != nil {
		return err
	}
	if w.outcome != want {
		return fmt.Errorf("expected outcome %v, got %v", want, w.outcome)
	}
	return nil
}

func (w *deliveryWorld) requestWasBasicJSONPost() error {
	if len(w.requests) == 0 {
		return fmt.Errorf("no request recorded")
	}
	r := w.requests[len(w.requests)-1]
	if r.method != http.MethodPost {
		return fmt.Errorf("expected POST, got %s", r.method)
	}
	if r.contentType != "application/json" {
		return fmt.Errorf("expected Content-Type application/json, got %q", r.contentType)
	}
	if !r.basicOK || r.username != w.destination.Username || r.password != w.destination.Password {
		return fmt.Errorf("expected Basic auth %s:%s, got ok=%v %s:%s", w.destination.Username, w.destination.Password, r.basicOK, r.username, r.password)
	}
	return nil
}

func (w *deliveryWorld) requestBodyMatchesContractEnvelope() error {
	if len(w.requests) == 0 {
		return fmt.Errorf("no request recorded")
	}
	want, err := json.Marshal(protocol.Envelope{
		DataSourceID:               w.source.ID,
		DataSourceDescription:      w.source.Description,
		DataDestinationID:          w.destination.ID,
		DataDestinationDescription: w.destination.Description,
		Payload:                    w.deliveredPayload,
	})
	if err != nil {
		return err
	}
	got := w.requests[len(w.requests)-1].body
	if string(got) != string(want) {
		return fmt.Errorf("request body = %s, want %s", got, want)
	}
	return nil
}

func (w *deliveryWorld) everySleepBetween1And60() error {
	if len(w.sleeps) == 0 {
		return fmt.Errorf("no sleeps recorded")
	}
	for _, d := range w.sleeps {
		if d < time.Second || d > 60*time.Second {
			return fmt.Errorf("sleep %v out of bounds [1s,60s]", d)
		}
	}
	return nil
}

func (w *deliveryWorld) deliveryFailedWithCancellation() error {
	if !errors.Is(w.err, context.Canceled) {
		return fmt.Errorf("expected context.Canceled, got %v", w.err)
	}
	return nil
}

func (w *deliveryWorld) outcomeIsZeroValue() error {
	if w.outcome != 0 {
		return fmt.Errorf("expected zero-value outcome, got %v", w.outcome)
	}
	return nil
}

func (w *deliveryWorld) requestCarriedDiagnosticHeaders() error {
	if len(w.requests) == 0 {
		return fmt.Errorf("no request recorded")
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
			return fmt.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	return nil
}

func (w *deliveryWorld) receiverRequestCountIs(n int) error {
	if got := len(w.requests); got != n {
		return fmt.Errorf("expected %d requests, got %d", n, got)
	}
	return nil
}

func (w *deliveryWorld) bindDestination() {
	var filter *protocol.Filter
	if w.destination.Filter != nil {
		filter = &protocol.Filter{Column: w.destination.Filter.Column, Values: w.destination.Filter.Values}
	}
	w.sender.Destination = delivery.Destination{ID: w.destination.ID, Description: w.destination.Description, Filter: filter}
	w.sender.Sender = httpdelivery.NewClient(nil, httpdelivery.Destination{ID: w.destination.ID, Endpoint: w.destination.Endpoint, Username: w.destination.Username, Password: w.destination.Password})
}
