package protocol

import "encoding/json"

type Filter struct {
	Column string
	Values []string
}

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
	for _, allowed := range filter.Values {
		if value == allowed {
			return true
		}
	}
	return false
}
