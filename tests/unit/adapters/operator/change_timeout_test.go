package operator_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/adapters/operator"
	"github.com/rafaeelricco/postie/internal/control"
)

// deadlineBackend records how long a change request is allowed to run.
type deadlineBackend struct {
	remaining time.Duration
	bounded   bool
}

func (b *deadlineBackend) Status() control.Status                { return control.Status{} }
func (b *deadlineBackend) Subscriptions() []control.Subscription { return nil }
func (b *deadlineBackend) Logs(string) (activity.Page, error)    { return activity.Page{}, nil }

func (b *deadlineBackend) Change(ctx context.Context, id string, state control.SubscriptionState) (control.Subscription, error) {
	deadline, ok := ctx.Deadline()
	b.bounded = ok
	b.remaining = time.Until(deadline)
	return control.Subscription{ID: id, State: state, DesiredState: state}, nil
}

// A pause or resume waits for every worker to converge, so the handler must
// give the backend a real window: bounded, but tens of seconds long.
func TestChangeRequestRunsUnderAThirtySecondDeadline(t *testing.T) {
	backend := &deadlineBackend{}
	req := httptest.NewRequest(http.MethodPost, "/v1/subscriptions/billing/pause", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()

	operator.New("secret", backend).Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !backend.bounded {
		t.Fatal("change request ran without a deadline")
	}
	if backend.remaining < 29*time.Second || backend.remaining > 30*time.Second {
		t.Fatalf("change deadline is %v away, want about 30s", backend.remaining)
	}
}
