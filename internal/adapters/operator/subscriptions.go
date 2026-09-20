package operator

import (
	"context"
	"errors"
	"github.com/rafaeelricco/postie/internal/control"
	"net/http"
	"strings"
	"time"
)

func (s *Server) subscription(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/subscriptions/"), "/")
	if len(parts) != 2 || (parts[1] != "pause" && parts[1] != "resume") || r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	state := control.StateRunning
	if parts[1] == "pause" {
		state = control.StatePaused
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	item, err := s.backend.Change(ctx, parts[0], state)
	if errors.Is(err, control.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		code := http.StatusServiceUnavailable
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			code = http.StatusGatewayTimeout
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		write(w, struct {
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
