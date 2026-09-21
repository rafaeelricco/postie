package operator

import (
	"context"
	"net/http"

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
