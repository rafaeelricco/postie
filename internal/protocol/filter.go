package protocol

import (
	"encoding/json"
	"slices"
)

// Filter keeps only records whose Column holds one of Values.
type Filter struct {
	Column string
	Values []string
}

// MatchesFilter reports whether a payload should be delivered. It fails open:
// no filter, an unreadable payload, or a non-string column all match, because
// dropping a record silently is worse than delivering one too many.
//
//	f := &Filter{Column: "tenant", Values: []string{"acme"}}
//	MatchesFilter(f, []byte(`{"tenant":"acme"}`))  // true
//	MatchesFilter(f, []byte(`{"tenant":"other"}`)) // false
//	MatchesFilter(f, []byte(`{"tenant":7}`))       // true
func MatchesFilter(filter *Filter, payload json.RawMessage) bool {
	if filter == nil {
		return true
	}
	var object map[string]any
	if json.Unmarshal(payload, &object) != nil {
		return true
	}
	value, ok := object[filter.Column].(string)
	if !ok {
		return true
	}
	return slices.Contains(filter.Values, value)
}
