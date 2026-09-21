package protocol

import (
	"encoding/json"
	"fmt"
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
