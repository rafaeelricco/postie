package operator

import (
	"context"
	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/control"
	"net/http"
)

type Backend interface {
	Status() control.Status
	Subscriptions() []control.Subscription
	Change(context.Context, string, control.SubscriptionState) (control.Subscription, error)
	Logs(string) (activity.Page, error)
}
type Server struct {
	token   string
	backend Backend
}

func New(token string, backend Backend) *Server { return &Server{token: token, backend: backend} }
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
