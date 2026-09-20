package httpdelivery

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
