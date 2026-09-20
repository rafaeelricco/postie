package protocol

import (
	"encoding/json"
	"testing"
)

func FuzzMatchesFilter(f *testing.F) {
	for _, seed := range []string{`{"event_name":"Created"}`, `{"event_name":"Deleted"}`, `{"event_name":3}`, `{"other":1}`, `"just text"`, `[1,2,3]`, `null`, `{not json`, ``} {
		f.Add(seed)
	}
	filter := &Filter{Column: "event_name", Values: []string{"Created"}}
	f.Fuzz(func(t *testing.T, payload string) {
		raw := json.RawMessage(payload)
		if !MatchesFilter(nil, raw) {
			t.Fatalf("nil filter must match %q", payload)
		}
		matched := MatchesFilter(filter, raw)
		var object map[string]any
		if json.Unmarshal(raw, &object) != nil && !matched {
			t.Fatalf("malformed payload did not fail open: %q", payload)
		}
	})
}
