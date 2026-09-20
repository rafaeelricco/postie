package protocol

import (
	"encoding/json"
	"testing"
)

// FuzzDecodeAcknowledgement checks that DecodeAcknowledgement never panics,
// that a returned error always implies Retry, and that a Success result
// always decodes a non-nil result.success object.
func FuzzDecodeAcknowledgement(f *testing.F) {
	// Seeds: the bodies in protocol_test.go:6-19.
	seeds := []string{
		`{"result":{"success":{}}}`,
		`{"result":{"error":{"policy":"must_retry","class":"c","description":"d"}}}`,
		`{"result":{"error":{"policy":"keep_going","class":"c","description":"d"}}}`,
		`{"result":{"success":null}}`,
		`{"result":{"error":{"policy":"keep_going"}}}`,
		`{"result":{"success":{},"error":{"policy":"must_retry","class":"c","description":"d"}}}`,
		`{"result":{}}`,
		``,
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, body string) {
		got, err := DecodeAcknowledgement([]byte(body))
		if err != nil {
			if got != Retry {
				t.Fatalf("body %q: err != nil must imply Retry, got %v with err %v", body, got, err)
			}
			return
		}
		if got != Success {
			return
		}
		var response struct {
			Result map[string]json.RawMessage `json:"result"`
		}
		if jsonErr := json.Unmarshal([]byte(body), &response); jsonErr != nil {
			t.Fatalf("body %q: Success but re-decoding the body failed: %v", body, jsonErr)
		}
		success, ok := response.Result["success"]
		if !ok {
			t.Fatalf("body %q: Success but result.success is absent", body)
		}
		var v map[string]any
		if jsonErr := json.Unmarshal(success, &v); jsonErr != nil || v == nil {
			t.Fatalf("body %q: Success but result.success does not decode to a non-nil object (err=%v, v=%v)", body, jsonErr, v)
		}
	})
}
