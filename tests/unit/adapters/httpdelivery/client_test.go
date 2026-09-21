package httpdelivery_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/adapters/httpdelivery"
	"github.com/rafaeelricco/postie/internal/delivery"
	"github.com/rafaeelricco/postie/internal/stream"
)

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
	client := httpdelivery.NewClient(nil, httpdelivery.Destination{ID: "d", Endpoint: server.URL, Username: "u", Password: "p"})
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
	client := httpdelivery.NewClient(nil, httpdelivery.Destination{})
	if client.HTTP == nil || client.HTTP.Timeout != 60*time.Second {
		t.Fatalf("expected default 60s client, got %+v", client.HTTP)
	}
	given := &http.Client{Timeout: 5 * time.Second}
	if got := httpdelivery.NewClient(given, httpdelivery.Destination{}); got.HTTP != given {
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
	client := httpdelivery.NewClient(nil, httpdelivery.Destination{ID: "d", Endpoint: server.URL})
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
	client := httpdelivery.NewClient(&http.Client{}, httpdelivery.Destination{ID: "d", Endpoint: destination.URL + "/redirect"})
	_, err := client.Send(context.Background(), stream.Record{}, nil)
	if err == nil || target != 0 {
		t.Fatalf("redirect result err=%v target=%d", err, target)
	}
}

type fakeGate struct {
	allowed  bool
	deadline time.Time
}

func (g fakeGate) Allowed(string) bool      { return g.allowed }
func (g fakeGate) LeaseDeadline() time.Time { return g.deadline }

func TestHTTPAttemptsRejectExpiredLease(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1:1", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = (httpdelivery.LeaseTransport{Gate: fakeGate{allowed: false, deadline: time.Now().Add(-time.Second)}}).RoundTrip(req)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}
