package protocol

import (
	"encoding/json"
	"fmt"
)

type Acknowledgement int

const (
	Retry Acknowledgement = iota
	Success
	KeepGoing
)

func DecodeAcknowledgement(body []byte) (Acknowledgement, error) {
	var response struct {
		Result map[string]json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return Retry, fmt.Errorf("invalid acknowledgement: %w", err)
	}
	if success, ok := response.Result["success"]; ok {
		var value map[string]any
		if json.Unmarshal(success, &value) == nil && value != nil {
			return Success, nil
		}
	}
	if raw, ok := response.Result["error"]; ok {
		var e struct {
			Policy      *string `json:"policy"`
			Class       *string `json:"class"`
			Description *string `json:"description"`
		}
		if json.Unmarshal(raw, &e) == nil && e.Policy != nil && e.Class != nil && e.Description != nil {
			switch *e.Policy {
			case "must_retry":
				return Retry, nil
			case "keep_going":
				return KeepGoing, nil
			}
		}
	}
	return Retry, fmt.Errorf("invalid acknowledgement")
}
