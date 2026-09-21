package bdd

import (
	"context"
	"encoding/json"
	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/control"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/rafaeelricco/postie/internal/adapters/operator"
)

// operatorWorld is built per scenario by newOperatorWorld, which closes its
// httptest server through t.Cleanup.
type operatorWorld struct {
	t      *testing.T
	server *httptest.Server

	status int
	body   []byte
}

const operatorToken = "secret-token"

// newOperatorWorld starts every scenario with an operator server that accepts
// operatorToken and knows the subscriptions "orders" and "invoices".
func newOperatorWorld(t *testing.T) *operatorWorld {
	t.Helper()
	server := httptest.NewServer(operator.New(operatorToken, newOperatorBackend("orders", "invoices")).Handler())
	t.Cleanup(server.Close)
	return &operatorWorld{t: t, server: server}
}

// docs/specification.md, "Delivery and operations": the private operator API.
func TestOperator(t *testing.T) {
	t.Run("health endpoints need no token", func(t *testing.T) {
		w := newOperatorWorld(t)
		w.do(http.MethodGet, "/health/live", "")
		w.responseStatusIs(200)
		w.do(http.MethodGet, "/health/ready", "")
		w.responseStatusIs(200)
	})

	t.Run("status requires the right token", func(t *testing.T) {
		w := newOperatorWorld(t)
		w.do(http.MethodGet, "/v1/status", "")
		w.responseStatusIs(401)
		w.do(http.MethodGet, "/v1/status", "wrong-token")
		w.responseStatusIs(401)
		w.do(http.MethodGet, "/v1/status", operatorToken)
		w.responseStatusIs(200)
	})

	t.Run("pausing a subscription is visible in the list and resume restores it", func(t *testing.T) {
		w := newOperatorWorld(t)
		w.do(http.MethodPost, "/v1/subscriptions/orders/pause", operatorToken)
		w.responseStatusIs(200)
		w.subscriptionListShows("orders", "paused")
		w.subscriptionListShows("invoices", "running")
		w.do(http.MethodPost, "/v1/subscriptions/orders/resume", operatorToken)
		w.responseStatusIs(200)
		w.subscriptionListShows("orders", "running")
	})

	t.Run("an unknown subscription is not found", func(t *testing.T) {
		w := newOperatorWorld(t)
		w.do(http.MethodPost, "/v1/subscriptions/missing/pause", operatorToken)
		w.responseStatusIs(404)
	})

	t.Run("recent engine activity requires authentication", func(t *testing.T) {
		w := newOperatorWorld(t)
		w.do(http.MethodGet, "/v1/logs", "")
		w.responseStatusIs(401)
		w.do(http.MethodGet, "/v1/logs", operatorToken)
		w.responseStatusIs(200)
		w.do(http.MethodGet, "/v1/logs?after=broken", operatorToken)
		w.responseStatusIs(400)
	})
}

func (w *operatorWorld) do(method, path, bearer string) {
	w.t.Helper()
	req, err := http.NewRequest(method, w.server.URL+path, nil)
	if err != nil {
		w.t.Fatal(err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		w.t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		w.t.Fatal(err)
	}
	w.status = resp.StatusCode
	w.body = body
}

func (w *operatorWorld) responseStatusIs(want int) {
	w.t.Helper()
	if w.status != want {
		w.t.Fatalf("expected status %d, got %d (body %s)", want, w.status, w.body)
	}
}

func (w *operatorWorld) subscriptionListShows(id, state string) {
	w.t.Helper()
	req, err := http.NewRequest(http.MethodGet, w.server.URL+"/v1/subscriptions", nil)
	if err != nil {
		w.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+operatorToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		w.t.Fatal(err)
	}
	defer resp.Body.Close()

	var subs []struct {
		ID    string `json:"id"`
		State string `json:"state"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&subs); err != nil {
		w.t.Fatal(err)
	}
	for _, s := range subs {
		if s.ID == id {
			if s.State != state {
				w.t.Fatalf("expected %q to be %q, got %q", id, state, s.State)
			}
			return
		}
	}
	w.t.Fatalf("subscription %q not found in list", id)
}

type operatorBackend struct {
	mu    sync.Mutex
	items map[string]control.Subscription
	log   *activity.Log
}

func newOperatorBackend(ids ...string) *operatorBackend {
	b := &operatorBackend{items: map[string]control.Subscription{}, log: activity.New(nil)}
	for _, id := range ids {
		b.items[id] = control.Subscription{ID: id, State: control.StateRunning}
	}
	return b
}
func (b *operatorBackend) Status() control.Status { return control.Status{Status: "ready"} }
func (b *operatorBackend) Subscriptions() []control.Subscription {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := []control.Subscription{}
	for _, v := range b.items {
		out = append(out, v)
	}
	return out
}
func (b *operatorBackend) Change(_ context.Context, id string, state control.SubscriptionState) (control.Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.items[id]
	if !ok {
		return v, control.ErrNotFound
	}
	v.State = state
	b.items[id] = v
	return v, nil
}
func (b *operatorBackend) Logs(cursor string) (activity.Page, error) { return b.log.Read(cursor) }
