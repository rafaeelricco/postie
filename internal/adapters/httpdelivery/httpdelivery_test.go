package httpdelivery

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/adapters/debezium"
	"github.com/rafaeelricco/postie/internal/delivery"
	"github.com/rafaeelricco/postie/internal/stream"
)

func processHTTP(ctx context.Context, client *Client, destination Destination, record stream.Record, sleep func(context.Context, time.Duration) error) (delivery.Outcome, error) {
	p := delivery.NewProcessor(delivery.Destination{ID: destination.ID, Description: destination.ID}, client)
	if sleep != nil {
		p.Sleep = sleep
	}
	return p.Process(ctx, record, nil)
}

func TestRetriesUntilSuccess(t *testing.T) {
	var count, sleeps int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch atomic.AddInt32(&count, 1) {
		case 1:
			w.WriteHeader(http.StatusInternalServerError)
		case 2:
			_, _ = w.Write([]byte(`{not json`))
		case 3:
		case 4:
			_, _ = w.Write([]byte(`{"result":{"error":{"policy":"must_retry","class":"c","description":"d"}}}`))
		default:
			_, _ = w.Write([]byte(`{"result":{"success":{}}}`))
		}
	}))
	defer server.Close()
	client := NewClient(nil, Destination{ID: "d", Endpoint: server.URL, Username: "u", Password: "p"})
	outcome, err := processHTTP(context.Background(), client, client.Destination, stream.Record{Source: stream.Source{ID: "s", Description: "s"}, Payload: []byte(`{}`)}, func(context.Context, time.Duration) error { atomic.AddInt32(&sleeps, 1); return nil })
	if err != nil || outcome != delivery.Delivered || count != 5 || sleeps != 4 {
		t.Fatalf("outcome=%v err=%v requests=%d sleeps=%d", outcome, err, count, sleeps)
	}
}

func TestKeepGoingSkips(t *testing.T) {
	var count int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&count, 1)
		_, _ = w.Write([]byte(`{"result":{"error":{"policy":"keep_going","class":"c","description":"d"}}}`))
	}))
	defer server.Close()
	client := NewClient(nil, Destination{ID: "d", Endpoint: server.URL})
	outcome, err := processHTTP(context.Background(), client, client.Destination, stream.Record{Payload: []byte(`{}`)}, func(context.Context, time.Duration) error { t.Fatal("unexpected retry"); return nil })
	if err != nil || outcome != delivery.Skipped || count != 1 {
		t.Fatalf("outcome=%v err=%v count=%d", outcome, err, count)
	}
}

func TestOversizeResponseRetries(t *testing.T) {
	oversize := []byte(fmt.Sprintf(`{"result":{"success":{"pad":"%s"}}}`, strings.Repeat("x", 70*1024)))
	var count, sleeps int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&count, 1) == 1 {
			_, _ = w.Write(oversize)
			return
		}
		_, _ = w.Write([]byte(`{"result":{"success":{}}}`))
	}))
	defer server.Close()
	client := NewClient(nil, Destination{ID: "d", Endpoint: server.URL})
	outcome, err := processHTTP(context.Background(), client, client.Destination, stream.Record{Payload: []byte(`{}`)}, func(context.Context, time.Duration) error { atomic.AddInt32(&sleeps, 1); return nil })
	if err != nil || outcome != delivery.Delivered || count != 2 || sleeps != 1 {
		t.Fatalf("outcome=%v err=%v requests=%d sleeps=%d", outcome, err, count, sleeps)
	}
}

func TestCancelStopsRetryLoop(t *testing.T) {
	var count int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient(nil, Destination{ID: "d", Endpoint: server.URL})
	outcome, err := processHTTP(ctx, client, client.Destination, stream.Record{Payload: []byte(`{}`)}, func(ctx context.Context, _ time.Duration) error { cancel(); return ctx.Err() })
	if !errors.Is(err, context.Canceled) || outcome != 0 || count != 1 {
		t.Fatalf("outcome=%v err=%v count=%d", outcome, err, count)
	}
}

func TestStableIDFallback(t *testing.T) {
	record := stream.Record{Topic: "events", Partition: 3, Offset: 42}
	id := stableID(record)
	if len(id) != 64 {
		t.Fatalf("expected 64 hex chars, got %d", len(id))
	}
	if _, err := hex.DecodeString(id); err != nil {
		t.Fatal(err)
	}
	if stableID(record) != id {
		t.Fatal("stable id changed")
	}
	bumped := record
	bumped.Offset++
	if stableID(bumped) == id {
		t.Fatal("offset change did not change id")
	}
	var got string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Idempotency-Key")
		_, _ = w.Write([]byte(`{"result":{"success":{}}}`))
	}))
	defer server.Close()
	client := NewClient(nil, Destination{ID: "dest", Endpoint: server.URL})
	if _, err := client.Send(context.Background(), record, nil); err != nil {
		t.Fatal(err)
	}
	if got != id+":dest:0" {
		t.Fatalf("idempotency key=%q", got)
	}
}

func TestResponseBodyLimitIsExactly64KiB(t *testing.T) {
	const limit = 64 << 10
	t.Run("exactly at the limit delivers", func(t *testing.T) {
		ack := `{"result":{"success":{}}}`
		body := ack + strings.Repeat(" ", limit-len(ack))
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
		defer server.Close()
		client := NewClient(nil, Destination{ID: "d", Endpoint: server.URL})
		outcome, err := client.Send(context.Background(), stream.Record{}, nil)
		if err != nil || outcome != delivery.Delivered {
			t.Fatalf("outcome=%v err=%v", outcome, err)
		}
	})
	t.Run("one byte over the limit retries", func(t *testing.T) {
		prefix, suffix := `{"result":{"success":{"pad":"`, `"}}}`
		body := prefix + strings.Repeat("x", limit+1-len(prefix)-len(suffix)) + suffix
		var count, sleeps int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if atomic.AddInt32(&count, 1) == 1 {
				_, _ = w.Write([]byte(body))
				return
			}
			_, _ = w.Write([]byte(`{"result":{"success":{}}}`))
		}))
		defer server.Close()
		client := NewClient(nil, Destination{ID: "d", Endpoint: server.URL})
		outcome, err := processHTTP(context.Background(), client, client.Destination, stream.Record{}, func(context.Context, time.Duration) error { atomic.AddInt32(&sleeps, 1); return nil })
		if err != nil || outcome != delivery.Delivered || count != 2 || sleeps != 1 {
			t.Fatalf("outcome=%v err=%v count=%d sleeps=%d", outcome, err, count, sleeps)
		}
	})
	for _, overflow := range []string{" ", "!"} {
		t.Run(fmt.Sprintf("valid ack plus %q past the limit is rejected", overflow), func(t *testing.T) {
			ack := `{"result":{"success":{}}}`
			body := ack + strings.Repeat(" ", limit-len(ack)) + overflow
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
			defer server.Close()
			client := NewClient(nil, Destination{ID: "d", Endpoint: server.URL})
			if _, err := client.Send(context.Background(), stream.Record{}, nil); err == nil || !strings.Contains(err.Error(), "exceeds 64 KiB") {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestClassifyStatusBoundaries(t *testing.T) {
	success := []byte(`{"result":{"success":{}}}`)
	for _, tc := range []struct {
		status int
		want   delivery.Outcome
	}{{199, 0}, {200, delivery.Delivered}, {299, delivery.Delivered}, {300, 0}} {
		outcome, err := classify(tc.status, success)
		if outcome != tc.want || (tc.want == 0) != (err != nil) {
			t.Fatalf("classify(%d)=%v,%v", tc.status, outcome, err)
		}
	}
}

func TestClassify(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		want   delivery.Outcome
		err    bool
	}{
		{200, `{"result":{"success":{}}}`, delivery.Delivered, false}, {200, `{"result":{"error":{"policy":"keep_going","class":"c","description":"d"}}}`, delivery.Skipped, false}, {200, `{"result":{"error":{"policy":"must_retry","class":"c","description":"d"}}}`, 0, true}, {200, ``, 0, true}, {200, `{not json`, 0, true}, {500, `{"result":{"success":{}}}`, 0, true}, {401, `{"error":"bad token"}`, 0, true},
	} {
		outcome, err := classify(tc.status, []byte(tc.body))
		if outcome != tc.want || (err != nil) != tc.err {
			t.Fatalf("classify(%d,%q)=%v,%v", tc.status, tc.body, outcome, err)
		}
	}
}

func TestDiagnosticHeaders(t *testing.T) {
	var got http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		_, _ = w.Write([]byte(`{"result":{"success":{}}}`))
	}))
	defer server.Close()
	record := stream.Record{Topic: "events", Partition: 3, Offset: 42, EventID: "evt-1", Generation: 2, Replay: true}
	client := NewClient(nil, Destination{ID: "dest", Endpoint: server.URL, Username: "u", Password: "p"})
	if _, err := client.Send(context.Background(), record, nil); err != nil {
		t.Fatal(err)
	}
	for header, want := range map[string]string{"X-Postie-Event-ID": "evt-1", "X-Postie-Delivery-Generation": "2", "X-Postie-Replay": "true", "X-Postie-Topic": "events", "X-Postie-Partition": "3", "X-Postie-Offset": "42", "Idempotency-Key": "evt-1:dest:2"} {
		if got.Get(header) != want {
			t.Errorf("%s=%q want %q", header, got.Get(header), want)
		}
	}
}

func TestClientHeadersAndAcknowledgements(t *testing.T) {
	var headers http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers = r.Header.Clone()
		if user, pass, ok := r.BasicAuth(); !ok || user != "u" || pass != "p" {
			t.Errorf("basic auth missing")
		}
		_, _ = w.Write([]byte(`{"result":{"success":{}}}`))
	}))
	defer server.Close()
	client := NewClient(nil, Destination{ID: "d", Endpoint: server.URL, Username: "u", Password: "p"})
	record := stream.Record{Topic: "events", Partition: 3, Offset: 42, EventID: "evt", Generation: 2, Replay: true}
	outcome, err := client.Send(context.Background(), record, []byte(`{}`))
	if err != nil || outcome != delivery.Delivered {
		t.Fatalf("got %v %v", outcome, err)
	}
	for key, want := range map[string]string{"X-Postie-Event-ID": "evt", "X-Postie-Delivery-Generation": "2", "X-Postie-Replay": "true", "X-Postie-Topic": "events", "X-Postie-Partition": "3", "X-Postie-Offset": "42", "Idempotency-Key": "evt:d:2"} {
		if got := headers.Get(key); got != want {
			t.Errorf("%s=%q want %q", key, got, want)
		}
	}
}

func TestNewClientDefaults(t *testing.T) {
	client := NewClient(nil, Destination{})
	if client.HTTP == nil || client.HTTP.Timeout != 60*time.Second {
		t.Fatalf("expected default 60s client, got %+v", client.HTTP)
	}
	given := &http.Client{Timeout: 5 * time.Second}
	if got := NewClient(given, Destination{}); got.HTTP != given {
		t.Fatal("expected supplied client to be retained")
	}
}

func TestClientRejectsOversizedAcknowledgement(t *testing.T) {
	const limit = 64 << 10
	var count int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&count, 1) == 1 {
			_, _ = w.Write([]byte(`{"result":{"success":{"pad":"` + strings.Repeat("x", limit) + `"}}}`))
			return
		}
		_, _ = w.Write([]byte(`{"result":{"success":{}}}`))
	}))
	defer server.Close()
	client := NewClient(nil, Destination{ID: "d", Endpoint: server.URL})
	_, err := client.Send(context.Background(), stream.Record{}, nil)
	if err == nil || !strings.Contains(err.Error(), "exceeds 64 KiB") || count != 1 {
		t.Fatalf("err=%v requests=%d", err, count)
	}
}

func TestClientDoesNotFollowRedirect(t *testing.T) {
	var target int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/target", http.StatusTemporaryRedirect)
			return
		}
		atomic.AddInt32(&target, 1)
		_, _ = w.Write([]byte(`{"result":{"success":{}}}`))
	}))
	defer destination.Close()
	client := NewClient(&http.Client{}, Destination{ID: "d", Endpoint: destination.URL + "/redirect"})
	_, err := client.Send(context.Background(), stream.Record{}, nil)
	if err == nil || target != 0 {
		t.Fatalf("redirect result err=%v target=%d", err, target)
	}
}

func TestHTTPAttemptsRejectExpiredLease(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1:1", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = (LeaseTransport{Gate: transportGate{allowed: false, deadline: time.Now().Add(-time.Second)}}).RoundTrip(req)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

type transportGate struct {
	allowed  bool
	deadline time.Time
}

func (g transportGate) Allowed(string) bool      { return g.allowed }
func (g transportGate) LeaseDeadline() time.Time { return g.deadline }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

type closeSignalBody struct{ closed chan struct{} }

func (b closeSignalBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b closeSignalBody) Close() error             { close(b.closed); return nil }

func TestLeaseTransportAppliesLeaseDeadline(t *testing.T) {
	wantDeadline := time.Now().Add(30 * time.Millisecond)
	seen := make(chan error, 1)
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		got, ok := req.Context().Deadline()
		if !ok || got.Before(wantDeadline.Add(-5*time.Millisecond)) || got.After(wantDeadline.Add(5*time.Millisecond)) {
			seen <- errors.New("lease deadline was not copied")
			return nil, errors.New("lease deadline was not copied")
		}
		<-req.Context().Done()
		seen <- req.Context().Err()
		return nil, req.Context().Err()
	})
	req, err := http.NewRequest(http.MethodGet, "http://example.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = (LeaseTransport{Gate: transportGate{allowed: true, deadline: wantDeadline}, Base: base}).RoundTrip(req)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RoundTrip error=%v", err)
	}
	if got := <-seen; !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("base context error=%v", got)
	}
}

func TestLeaseTransportCancelsInFlightBodyWhenClosed(t *testing.T) {
	bodyClosed := make(chan struct{})
	baseSawCancel := make(chan struct{})
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		go func() { <-req.Context().Done(); close(baseSawCancel) }()
		return &http.Response{StatusCode: http.StatusOK, Body: closeSignalBody{closed: bodyClosed}, Request: req}, nil
	})
	req, err := http.NewRequest(http.MethodGet, "http://example.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := (LeaseTransport{Gate: transportGate{allowed: true, deadline: time.Now().Add(time.Minute)}, Base: base}).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-bodyClosed:
	case <-time.After(time.Second):
		t.Fatal("wrapped body did not close")
	}
	select {
	case <-baseSawCancel:
	case <-time.After(time.Second):
		t.Fatal("lease context was not cancelled on body close")
	}
}

func TestLeaseTransportPropagatesRequestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	done := make(chan error, 1)
	go func() {
		_, err := (LeaseTransport{Gate: transportGate{allowed: true, deadline: time.Now().Add(time.Minute)}, Base: base}).RoundTrip(req)
		done <- err
	}()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RoundTrip error=%v", err)
	}
}

func TestDecodeFormatSendPipeline(t *testing.T) {
	for _, op := range []string{"c", "r"} {
		t.Run(op, func(t *testing.T) {
			source := stream.Source{ID: "events", Description: "Source events", Table: "events", Columns: []string{"id", "event_id", "correlation_id", "recorded_on", "score", "payload", "binary"}, SerialColumn: "id", PartitioningColumn: "correlation_id"}
			identity := stream.Identity{Table: "events", SerialColumn: "id", PartitioningColumn: "correlation_id", EventIDColumn: "event_id", Partitions: 3, Columns: []stream.Column{{Name: "id", Type: stream.PGInt8}, {Name: "event_id", Type: stream.PGText}, {Name: "correlation_id", Type: stream.PGText}, {Name: "recorded_on", Type: stream.PGTimestamptz}, {Name: "score", Type: stream.PGFloat8}, {Name: "payload", Type: stream.PGJSON}, {Name: "binary", Type: stream.PGBytea}}}
			raw := &stream.RawRecord{Topic: "events-topic", Partition: 2, Offset: 41, LeaderEpoch: 7, Key: []byte(`{"correlation_id":"group"}`), Value: []byte(`{"source":{"schema":"public","table":"events"},"op":"` + op + `","after":{"id":9223372036854775807,"event_id":"event-1","correlation_id":"group","recorded_on":"2024-01-02T03:04:05.123456Z","score":1.25e1,"payload":"{\"z\":2,\"a\":1}","binary":"3q2+7w=="}}`)}
			record, err := debezium.Decode(source, identity, 2, raw)
			if err != nil {
				t.Fatal(err)
			}
			if record.LeaderEpoch != 7 {
				t.Fatalf("leader epoch=%d", record.LeaderEpoch)
			}
			type request struct {
				body                   []byte
				header                 http.Header
				method, user, password string
			}
			received := make(chan request, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				user, password, _ := r.BasicAuth()
				received <- request{body, r.Header.Clone(), r.Method, user, password}
				_, _ = io.WriteString(w, `{"result":{"success":{}}}`)
			}))
			defer server.Close()
			sender := NewClient(nil, Destination{ID: "projection", Endpoint: server.URL, Username: "user", Password: "secret"})
			processor := delivery.NewProcessor(delivery.Destination{ID: "projection", Description: "Read model"}, sender)
			processor.Sleep = func(context.Context, time.Duration) error { t.Error("unexpected retry"); return context.Canceled }
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			outcome, err := processor.Process(ctx, record, nil)
			if err != nil || outcome != delivery.Delivered {
				t.Fatalf("outcome=%v error=%v", outcome, err)
			}
			got := <-received
			const expected = `{"data_source_id":"events","data_source_description":"Source events","data_destination_id":"projection","data_destination_description":"Read model","payload":{"id":9223372036854775807,"event_id":"event-1","correlation_id":"group","recorded_on":"2024-01-02 03:04:05.123456+00","score":12.5,"payload":"{\"z\":2,\"a\":1}","binary":"3q2+7w=="}}`
			decode := func(raw []byte) any {
				t.Helper()
				var value any
				d := json.NewDecoder(bytes.NewReader(raw))
				d.UseNumber()
				if err := d.Decode(&value); err != nil {
					t.Fatal(err)
				}
				return value
			}
			if !reflect.DeepEqual(decode(got.body), decode([]byte(expected))) {
				t.Fatalf("body=%s, want %s", got.body, expected)
			}
			if got.method != "POST" || got.user != "user" || got.password != "secret" {
				t.Fatalf("request method/credentials=%s %s %s", got.method, got.user, got.password)
			}
			for name, want := range map[string]string{"Content-Type": "application/json", "X-Postie-Event-ID": "event-1", "X-Postie-Delivery-Generation": "2", "X-Postie-Topic": "events-topic", "X-Postie-Partition": "2", "X-Postie-Offset": "41", "Idempotency-Key": "event-1:projection:2"} {
				if got.header.Get(name) != want {
					t.Errorf("%s=%q, want %q", name, got.header.Get(name), want)
				}
			}
		})
	}
}

var errRetried = errors.New("retried")

func TestRegression_RedirectsRetryOriginalPOST(t *testing.T) {
	statuses := []int{http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect}
	for _, status := range statuses {
		t.Run(http.StatusText(status), func(t *testing.T) {
			for _, supplied := range []bool{false, true} {
				t.Run(map[bool]string{false: "default client", true: "supplied client"}[supplied], func(t *testing.T) {
					var originalCount, targetCount int32
					type request struct {
						method, body, username, password, idempotency string
					}
					var requests []request
					var requestsMu sync.Mutex
					target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						atomic.AddInt32(&targetCount, 1)
						_, _ = w.Write([]byte(`{"result":{"success":{}}}`))
					}))
					defer target.Close()
					original := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						atomic.AddInt32(&originalCount, 1)
						body, _ := io.ReadAll(r.Body)
						username, password, _ := r.BasicAuth()
						requestsMu.Lock()
						requests = append(requests, request{r.Method, string(body), username, password, r.Header.Get("Idempotency-Key")})
						requestsMu.Unlock()
						w.Header().Set("Location", target.URL)
						w.WriteHeader(status)
					}))
					defer original.Close()

					var client *http.Client
					var transport *regressionRoundTripper
					var callbackCalls int32
					var jar http.CookieJar
					if supplied {
						cookieJar, err := cookiejar.New(nil)
						if err != nil {
							t.Fatal(err)
						}
						jar = cookieJar
						transport = &regressionRoundTripper{base: http.DefaultTransport}
						client = &http.Client{
							Transport: transport,
							Jar:       jar,
							Timeout:   17 * time.Second,
							CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
								atomic.AddInt32(&callbackCalls, 1)
								return nil
							},
						}
					}
					destination := Destination{ID: "destination", Endpoint: original.URL, Username: "user", Password: "pass"}

					sender := delivery.NewProcessor(delivery.Destination{ID: destination.ID, Description: "destination"}, NewClient(client, destination))
					sleeps := 0
					sender.Sleep = func(context.Context, time.Duration) error {
						sleeps++
						if sleeps == 1 {
							return nil
						}
						return errRetried
					}
					record := stream.Record{Source: stream.Source{ID: "source", Description: "source"}, Payload: []byte(`{"value":"payload"}`), Topic: "topic", Partition: 4, Offset: 7, EventID: "event-id", Generation: 3}
					var before http.Client
					if supplied {
						before = *client
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					_, err := sender.Process(ctx, record, nil)
					if !errors.Is(err, errRetried) {
						t.Fatalf("Process() error = %v, want errRetried", err)
					}
					if got := atomic.LoadInt32(&originalCount); got != 2 {
						t.Fatalf("original request count = %d, want 2", got)
					}
					if got := atomic.LoadInt32(&targetCount); got != 0 {
						t.Fatalf("redirect target request count = %d, want 0", got)
					}
					requestsMu.Lock()
					gotRequests := append([]request(nil), requests...)
					requestsMu.Unlock()
					if sleeps != 2 || len(gotRequests) != 2 {
						t.Fatalf("sleeps=%d requests=%d, want 2 and 2", sleeps, len(gotRequests))
					}
					wantKey := "event-id:destination:3"
					wantBody := `{"data_source_id":"source","data_source_description":"source","data_destination_id":"destination","data_destination_description":"destination","payload":{"value":"payload"}}`
					for i, got := range gotRequests {
						if got.method != http.MethodPost || got.body != wantBody || got.username != "user" || got.password != "pass" || got.idempotency != wantKey {
							t.Errorf("request %d = %+v, want original POST/body/basic auth/idempotency %q", i, got, wantKey)
						}
					}
					if supplied {
						if got := transport.calls.Load(); got != 2 {
							t.Fatalf("supplied transport calls = %d, want 2", got)
						}
						if client.Timeout != before.Timeout || client.Transport != before.Transport || client.Jar != before.Jar || reflect.ValueOf(client.CheckRedirect).Pointer() != reflect.ValueOf(before.CheckRedirect).Pointer() {
							t.Fatal("supplied http.Client settings changed while sending")
						}
						if got := atomic.LoadInt32(&callbackCalls); got != 0 {
							t.Fatalf("supplied CheckRedirect callback called during sender.Process: %d", got)
						}
						response, err := client.Get(original.URL)
						if err != nil {
							t.Fatalf("independent supplied client request failed: %v", err)
						}
						response.Body.Close()
						if got := atomic.LoadInt32(&targetCount); got != 1 {
							t.Fatalf("independent supplied client target requests = %d, want 1", got)
						}
						if got := atomic.LoadInt32(&callbackCalls); got != 1 {
							t.Fatalf("independent supplied client callback calls = %d, want 1", got)
						}
					}
				})
			}
		})
	}
}

type regressionRoundTripper struct {
	base  http.RoundTripper
	calls atomic.Int32
}

func (t *regressionRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	t.calls.Add(1)
	return t.base.RoundTrip(r)
}
