package operator

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/rafaeelricco/postie/internal/control"
)

// subscription handles POST /v1/subscriptions/{id}/pause and .../resume.
func (s *Server) subscription(w http.ResponseWriter, r *http.Request) {
	id, state, ok := subscriptionAction(r.URL.Path)
	if !ok || r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	item, err := s.backend.Change(ctx, id, state)
	if errors.Is(err, control.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		writeStatus(w, changeFailureStatus(err), struct {
			Error        string               `json:"error"`
			Subscription control.Subscription `json:"subscription"`
		}{"control operation incomplete", item})
		return
	}
	write(w, item)
}

func (s *Server) subscriptions(w http.ResponseWriter, r *http.Request) {
	write(w, s.backend.Subscriptions())
}

// subscriptionAction parses "/v1/subscriptions/{id}/{pause|resume}" into the
// subscription id and the state the action asks for.
//
//	subscriptionAction("/v1/subscriptions/billing/pause") // "billing", StatePaused, true
//	subscriptionAction("/v1/subscriptions/billing")       // "", "", false
func subscriptionAction(path string) (id string, state control.SubscriptionState, ok bool) {
	parts := strings.Split(strings.TrimPrefix(path, "/v1/subscriptions/"), "/")
	if len(parts) != 2 {
		return "", "", false
	}
	switch parts[1] {
	case "pause":
		return parts[0], control.StatePaused, true
	case "resume":
		return parts[0], control.StateRunning, true
	default:
		return "", "", false
	}
}

// changeFailureStatus is 504 when the request ended before every worker
// converged, and 503 when the control store could not answer or a newer
// request replaced this one.
func changeFailureStatus(err error) int {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return http.StatusGatewayTimeout
	}
	return http.StatusServiceUnavailable
}
