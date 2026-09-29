package operator

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/control"
)

// Backend is what the operator HTTP API exposes: the engine's status, its
// subscriptions, and the ability to change one and to read the activity log.
type Backend interface {
	Status() control.Status
	Subscriptions() []control.Subscription
	Change(context.Context, string, control.SubscriptionState) (control.Subscription, error)
	Logs(string) (activity.Page, error)
}

// Server is the operator HTTP API: read-only status plus pause/resume for a
// running engine's Backend.
type Server struct {
	token   string
	backend Backend
}

// New builds a Server. token gates every route under /v1; an empty token
// rejects every request to those routes.
func New(token string, backend Backend) *Server { return &Server{token: token, backend: backend} }

// Handler builds the operator's route table. Only the pause and resume routes
// check the method; the rest answer any method.
//
//	/health/live                                process is up, no token
//	/health/ready                               engine reports "ready", no token
//	/v1/status                                  token required
//	/v1/subscriptions                           token required
//	/v1/subscriptions/{id}/{pause|resume} POST  token required
//	/v1/logs?after={cursor}                     token required
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health/live", s.live)
	mux.HandleFunc("/health/ready", s.ready)
	mux.HandleFunc("/v1/status", s.auth(s.status))
	mux.HandleFunc("/v1/subscriptions", s.auth(s.subscriptions))
	mux.HandleFunc("/v1/subscriptions/", s.auth(s.subscription))
	mux.HandleFunc("/v1/logs", s.auth(s.logs))
	return mux
}

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		value := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if s.token == "" || subtle.ConstantTimeCompare([]byte(value), []byte(s.token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (s *Server) logs(w http.ResponseWriter, r *http.Request) {
	page, err := s.backend.Logs(r.URL.Query().Get("after"))
	if err != nil {
		http.Error(w, "invalid log cursor", http.StatusBadRequest)
		return
	}
	write(w, page)
}

// live answers 200 for as long as the process is up.
func (s *Server) live(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }

// ready answers 200 only while the engine reports "ready", else 503.
func (s *Server) ready(w http.ResponseWriter, _ *http.Request) {
	if s.backend.Status().Status != "ready" {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) status(w http.ResponseWriter, _ *http.Request) { write(w, s.backend.Status()) }

// write sends value as a 200 JSON response.
func write(w http.ResponseWriter, value any) { writeStatus(w, http.StatusOK, value) }

// writeStatus sends value as JSON with the given status. It is the only JSON
// writer, and it sets the header before the status line, because a header set
// afterwards is silently dropped.
func writeStatus(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

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
