package operator

import (
	"encoding/json"
	"net/http"
)

func (s *Server) live(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if s.backend.Status().Status != "ready" {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}
func (s *Server) status(w http.ResponseWriter, r *http.Request) { write(w, s.backend.Status()) }
func write(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
