package bdd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/control"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"

	"github.com/cucumber/godog"

	"github.com/rafaeelricco/postie/internal/adapters/operator"
)

// operatorWorld is rebuilt in sc.Before for every scenario; its httptest
// server is closed in sc.After.
type operatorWorld struct {
	server *httptest.Server
	token  string

	status int
	body   []byte
}

func registerOperatorSteps(sc *godog.ScenarioContext) {
	w := &operatorWorld{}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		*w = operatorWorld{}
		return ctx, nil
	})
	sc.After(func(ctx context.Context, _ *godog.Scenario, err error) (context.Context, error) {
		if w.server != nil {
			w.server.Close()
		}
		return ctx, err
	})

	sc.Given(`^an operator server with token "([^"]*)" and subscriptions "([^"]*)", "([^"]*)"$`, w.anOperatorServer)

	sc.When(`^I GET "([^"]*)" with no token$`, w.getNoToken)
	sc.When(`^I GET "([^"]*)" with token "([^"]*)"$`, w.getWithToken)
	sc.When(`^I POST "([^"]*)" with token "([^"]*)"$`, w.postWithToken)

	sc.Then(`^the response status is (\d+)$`, w.responseStatusIs)
	sc.Then(`^the subscription list shows "([^"]*)" as "([^"]*)"$`, w.subscriptionListShows)
}

func (w *operatorWorld) anOperatorServer(token, id1, id2 string) error {
	w.token = token
	w.server = httptest.NewServer(operator.New(token, newOperatorBackend(id1, id2)).Handler())
	return nil
}

func (w *operatorWorld) do(method, path, bearer string) error {
	req, err := http.NewRequest(method, w.server.URL+path, nil)
	if err != nil {
		return err
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	w.status = resp.StatusCode
	w.body = body
	return nil
}

func (w *operatorWorld) getNoToken(path string) error {
	return w.do(http.MethodGet, path, "")
}

func (w *operatorWorld) getWithToken(path, token string) error {
	return w.do(http.MethodGet, path, token)
}

func (w *operatorWorld) postWithToken(path, token string) error {
	return w.do(http.MethodPost, path, token)
}

func (w *operatorWorld) responseStatusIs(want int) error {
	if w.status != want {
		return fmt.Errorf("expected status %d, got %d (body %s)", want, w.status, w.body)
	}
	return nil
}

func (w *operatorWorld) subscriptionListShows(id, state string) error {
	req, err := http.NewRequest(http.MethodGet, w.server.URL+"/v1/subscriptions", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+w.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var subs []struct {
		ID    string `json:"id"`
		State string `json:"state"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&subs); err != nil {
		return err
	}
	for _, s := range subs {
		if s.ID == id {
			if s.State != state {
				return fmt.Errorf("expected %q to be %q, got %q", id, state, s.State)
			}
			return nil
		}
	}
	return fmt.Errorf("subscription %q not found in list", id)
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
