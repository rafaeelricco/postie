package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/control"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// serve runs one request against s.Handler() through httptest.NewRecorder.
// bearer is the literal Authorization header value to send; an empty string
// omits the header entirely.
func serve(t *testing.T, s *Server, method, path, bearer string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	if bearer != "" {
		req.Header.Set("Authorization", bearer)
	}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

func TestHealthNeedsNoToken(t *testing.T) {
	s := testServer("secret", nil)
	for _, path := range []string{"/health/live", "/health/ready"} {
		t.Run(path, func(t *testing.T) {
			rec := serve(t, s, http.MethodGet, path, "")
			if rec.Code != http.StatusOK {
				t.Fatalf("%s with no Authorization header: got %d, want %d", path, rec.Code, http.StatusOK)
			}
		})
	}
}

func TestAuth(t *testing.T) {
	const token = "s3cr3t-token"
	s := testServer(token, []string{"a", "b"})

	authCases := []struct {
		name   string
		header string // Authorization header value; "" omits the header
		want   int
	}{
		{"no header", "", http.StatusUnauthorized},
		{"wrong token", "Bearer wrongtoken", http.StatusUnauthorized},
		{"Bearer + prefix of token", "Bearer " + token[:len(token)-1], http.StatusUnauthorized},
		// auth.go's auth computes value := strings.TrimPrefix(header, "Bearer ").
		// TrimPrefix returns a string unchanged when it does not have the given prefix, so a
		// raw token sent with no "Bearer " scheme passes through untouched, compares equal to
		// s.token, and authenticates today. This pins that actual behaviour so a later
		// rewrite of auth has to decide the raw-token-without-scheme case on purpose instead
		// of silently keeping or breaking it.
		{"raw token without scheme", token, http.StatusOK},
		{"right token", "Bearer " + token, http.StatusOK},
	}

	targets := []struct {
		name   string
		method string
		path   string
	}{
		{"status", http.MethodGet, "/v1/status"},
		{"subscriptions", http.MethodGet, "/v1/subscriptions"},
		{"subscription pause", http.MethodPost, "/v1/subscriptions/a/pause"},
	}

	for _, target := range targets {
		for _, tc := range authCases {
			t.Run(target.name+"/"+tc.name, func(t *testing.T) {
				rec := serve(t, s, target.method, target.path, tc.header)
				if rec.Code != tc.want {
					t.Fatalf("%s %s with Authorization=%q: got %d, want %d", target.method, target.path, tc.header, rec.Code, tc.want)
				}
			})
		}
	}
}

func TestEmptyServerTokenRejectsEverything(t *testing.T) {
	s := testServer("", []string{"a"})
	headers := []string{"", "Bearer ", "Bearer anything", "anything"}
	for _, header := range headers {
		t.Run("Authorization="+header, func(t *testing.T) {
			rec := serve(t, s, http.MethodGet, "/v1/status", header)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("Authorization=%q against empty server token: got %d, want %d", header, rec.Code, http.StatusUnauthorized)
			}
		})
	}
}

func TestPauseResume(t *testing.T) {
	const token = "tok"
	s := testServer(token, []string{"a", "b"})
	bearer := "Bearer " + token

	rec := serve(t, s, http.MethodPost, "/v1/subscriptions/a/pause", bearer)
	if rec.Code != http.StatusOK {
		t.Fatalf("pause a: got %d, want %d", rec.Code, http.StatusOK)
	}
	var paused control.Subscription
	if err := json.Unmarshal(rec.Body.Bytes(), &paused); err != nil {
		t.Fatalf("decode pause response: %v", err)
	}
	if paused != (control.Subscription{ID: "a", State: control.StatePaused}) {
		t.Fatalf("pause response = %+v, want {ID:a State:paused}", paused)
	}

	rec = serve(t, s, http.MethodGet, "/v1/subscriptions", bearer)
	if rec.Code != http.StatusOK {
		t.Fatalf("list: got %d, want %d", rec.Code, http.StatusOK)
	}
	var all []control.Subscription
	if err := json.Unmarshal(rec.Body.Bytes(), &all); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	byID := make(map[string]control.Subscription, len(all))
	for _, sub := range all {
		byID[sub.ID] = sub
	}
	if byID["a"].State != control.StatePaused {
		t.Fatalf("after pause, list shows a as %q, want %q", byID["a"].State, control.StatePaused)
	}
	if byID["b"].State != control.StateRunning {
		t.Fatalf("b must be untouched by pausing a: got %q, want %q", byID["b"].State, control.StateRunning)
	}

	rec = serve(t, s, http.MethodPost, "/v1/subscriptions/a/resume", bearer)
	if rec.Code != http.StatusOK {
		t.Fatalf("resume a: got %d, want %d", rec.Code, http.StatusOK)
	}
	var resumed control.Subscription
	if err := json.Unmarshal(rec.Body.Bytes(), &resumed); err != nil {
		t.Fatalf("decode resume response: %v", err)
	}
	if resumed != (control.Subscription{ID: "a", State: control.StateRunning}) {
		t.Fatalf("resume response = %+v, want {ID:a State:running}", resumed)
	}

	rec = serve(t, s, http.MethodGet, "/v1/subscriptions", bearer)
	all = nil
	if err := json.Unmarshal(rec.Body.Bytes(), &all); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	byID = make(map[string]control.Subscription, len(all))
	for _, sub := range all {
		byID[sub.ID] = sub
	}
	if byID["a"].State != control.StateRunning {
		t.Fatalf("after resume, list shows a as %q, want %q", byID["a"].State, control.StateRunning)
	}
	if byID["b"].State != control.StateRunning {
		t.Fatalf("b must still be untouched: got %q, want %q", byID["b"].State, control.StateRunning)
	}
}

func TestSubscriptionNotFound(t *testing.T) {
	const token = "tok"
	s := testServer(token, []string{"a"})
	bearer := "Bearer " + token

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"unknown id", http.MethodPost, "/v1/subscriptions/unknown/pause"},
		{"GET instead of POST", http.MethodGet, "/v1/subscriptions/a/pause"},
		{"unsupported action", http.MethodPost, "/v1/subscriptions/a/delete"},
		{"no action segment", http.MethodPost, "/v1/subscriptions/a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := serve(t, s, tc.method, tc.path, bearer)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("%s %s: got %d, want %d", tc.method, tc.path, rec.Code, http.StatusNotFound)
			}
		})
	}
}

func TestResponsesAreJSON(t *testing.T) {
	const token = "tok"
	s := testServer(token, []string{"a"})
	bearer := "Bearer " + token

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"status", http.MethodGet, "/v1/status"},
		{"list", http.MethodGet, "/v1/subscriptions"},
		{"pause", http.MethodPost, "/v1/subscriptions/a/pause"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := serve(t, s, tc.method, tc.path, bearer)
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("%s %s: Content-Type = %q, want %q", tc.method, tc.path, got, "application/json")
			}
		})
	}
}

func TestConcurrentPauseResumeIsRaceFree(t *testing.T) {
	const token = "tok"
	s := testServer(token, []string{"a"})
	bearer := "Bearer " + token

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			action := "pause"
			if i%2 == 0 {
				action = "resume"
			}
			serve(t, s, http.MethodPost, "/v1/subscriptions/a/"+action, bearer)
		}(i)
	}
	wg.Wait()

	rec := serve(t, s, http.MethodGet, "/v1/subscriptions", bearer)
	var all []control.Subscription
	if err := json.Unmarshal(rec.Body.Bytes(), &all); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected exactly 1 subscription after concurrent pause/resume, got %d", len(all))
	}
	if all[0].State != control.StatePaused && all[0].State != control.StateRunning {
		t.Fatalf("subscription state corrupted by concurrent access: %q", all[0].State)
	}
}

// These fakes test the HTTP adapter; runtime integration tests verify durable worker state.
type fakeBackend struct {
	mu        sync.Mutex
	items     map[string]control.Subscription
	status    control.Status
	logs      *activity.Log
	changeErr error
}

func testServer(token string, ids []string) *Server {
	b := &fakeBackend{items: map[string]control.Subscription{}, status: control.Status{Status: "ready"}, logs: activity.New(nil)}
	for _, id := range ids {
		b.items[id] = control.Subscription{ID: id, State: control.StateRunning}
	}
	return New(token, b)
}
func (b *fakeBackend) Status() control.Status { return b.status }
func (b *fakeBackend) Subscriptions() []control.Subscription {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := []control.Subscription{}
	for _, v := range b.items {
		out = append(out, v)
	}
	return out
}
func (b *fakeBackend) Change(ctx context.Context, id string, state control.SubscriptionState) (control.Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	item, ok := b.items[id]
	if !ok {
		return item, control.ErrNotFound
	}
	if b.changeErr != nil {
		return item, b.changeErr
	}
	item.State = state
	b.items[id] = item
	return item, nil
}
func (b *fakeBackend) Logs(cursor string) (activity.Page, error) { return b.logs.Read(cursor) }
func TestLiveReadinessAndLogs(t *testing.T) {
	s := testServer("tok", []string{"a"})
	b := s.backend.(*fakeBackend)
	b.status = control.Status{Status: "degraded"}
	if r := serve(t, s, "GET", "/health/ready", ""); r.Code != 503 {
		t.Fatalf("ready %d", r.Code)
	}
	if r := serve(t, s, "GET", "/health/live", ""); r.Code != 200 {
		t.Fatalf("live %d", r.Code)
	}
	if r := serve(t, s, "GET", "/v1/logs", ""); r.Code != 401 {
		t.Fatal("logs lack auth")
	}
	b.logs.Add(activity.Entry{Message: "committed", EventID: "event1"})
	r := serve(t, s, "GET", "/v1/logs", "Bearer tok")
	var page activity.Page
	if err := json.Unmarshal(r.Body.Bytes(), &page); err != nil || len(page.Entries) != 1 || page.Entries[0].EventID != "event1" {
		t.Fatalf("logs %s", r.Body.String())
	}
	if r := serve(t, s, "GET", "/v1/logs?after=invalid", "Bearer tok"); r.Code != 400 {
		t.Fatalf("bad cursor %d", r.Code)
	}
	for _, test := range []struct {
		err    error
		status int
	}{{context.DeadlineExceeded, 504}, {errors.New("secret dependency response"), 503}} {
		b.changeErr = test.err
		r := serve(t, s, "POST", "/v1/subscriptions/a/pause", "Bearer tok")
		if r.Code != test.status || bytes.Contains(r.Body.Bytes(), []byte("secret")) {
			t.Fatalf("operation error %d %s", r.Code, r.Body.String())
		}
	}
}
