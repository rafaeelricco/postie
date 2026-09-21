package operator

import (
	"encoding/json"
	"net/http"
)

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
