package protocol

import (
	"encoding/json"
	"fmt"
	"slices"
)

// Acknowledgement is what a destination told Postie to do with one record.
// Retry is the zero value on purpose: an Acknowledgement nobody set is the
// safe one.
type Acknowledgement int

const (
	Retry     Acknowledgement = iota // send the same record again; also the answer to anything malformed
	Success                          // delivered; commit and move on
	KeepGoing                        // rejected for good; audit the skip, then move on
)

// DecodeAcknowledgement reads a destination's response body. Anything it
// cannot understand is a Retry with an error: a confused destination must
// never cause a record to be skipped.
//
//	DecodeAcknowledgement([]byte(`{"result":{"success":{}}}`)) // Success
//	DecodeAcknowledgement([]byte(`{"result":{"error":{"policy":"keep_going","class":"x","description":"y"}}}`)) // KeepGoing
//	DecodeAcknowledgement([]byte(`{}`)) // Retry, "invalid acknowledgement"
func DecodeAcknowledgement(body []byte) (Acknowledgement, error) {
	var response struct {
		Result map[string]json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return Retry, fmt.Errorf("invalid acknowledgement: %w", err)
	}
	if isSuccess(response.Result["success"]) {
		return Success, nil
	}
	if policy, ok := errorPolicy(response.Result["error"]); ok {
		return policy, nil
	}
	return Retry, fmt.Errorf("invalid acknowledgement")
}

// isSuccess reports whether raw is a non-null JSON object, the shape a
// destination sends as {"result":{"success":{}}}. A missing "success" key
// gives a nil raw, which json.Unmarshal rejects.
func isSuccess(raw json.RawMessage) bool {
	var value map[string]any
	return json.Unmarshal(raw, &value) == nil && value != nil
}

// errorPolicy reads a destination's structured error and maps its policy to
// an Acknowledgement. ok is false when raw is missing, malformed, incomplete, or
// names a policy Postie does not recognize; the caller then falls back to
// Retry with an error.
func errorPolicy(raw json.RawMessage) (Acknowledgement, bool) {
	var e struct {
		Policy      *string `json:"policy"`
		Class       *string `json:"class"`
		Description *string `json:"description"`
	}
	if json.Unmarshal(raw, &e) != nil || e.Policy == nil || e.Class == nil || e.Description == nil {
		return Retry, false
	}
	switch *e.Policy {
	case "must_retry":
		return Retry, true
	case "keep_going":
		return KeepGoing, true
	default:
		return Retry, false
	}
}

// Envelope is the HTTP payload sent to a Postie destination.
type Envelope struct {
	DataSourceID               string          `json:"data_source_id"`
	DataSourceDescription      string          `json:"data_source_description"`
	DataDestinationID          string          `json:"data_destination_id"`
	DataDestinationDescription string          `json:"data_destination_description"`
	Payload                    json.RawMessage `json:"payload"`
}

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
	var object map[string]json.RawMessage
	if json.Unmarshal(payload, &object) != nil {
		return true
	}
	raw, ok := object[filter.Column]
	if !ok {
		return true
	}
	// A *string separates JSON null from a string: decoding null into a
	// string succeeds and would silently compare "", so it must fail open
	// like a number or an object does.
	var value *string
	if json.Unmarshal(raw, &value) != nil || value == nil {
		return true
	}
	return slices.Contains(filter.Values, *value)
}
