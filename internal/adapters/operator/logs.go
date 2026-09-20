package operator

import "net/http"

func (s *Server) logs(w http.ResponseWriter, r *http.Request) {
	page, err := s.backend.Logs(r.URL.Query().Get("after"))
	if err != nil {
		http.Error(w, "invalid log cursor", http.StatusBadRequest)
		return
	}
	write(w, page)
}
