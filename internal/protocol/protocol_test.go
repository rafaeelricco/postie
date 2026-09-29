package protocol

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/rafaeelricco/postie/internal/stream"
)

func TestConvertValuePreservesDecimalAndTimeSemantics(t *testing.T) {
	cases := []struct {
		typ       stream.PGType
		raw, want string
	}{
		{stream.PGFloat8, `1e300`, "1" + strings.Repeat("0", 300)},
		{stream.PGFloat4, `1.2e2`, `120`},
		{stream.PGTimestamp, `1704164645123456`, `"2024-01-02 03:04:05.123456"`},
		{stream.PGTimestamptz, `"2024-01-02T03:04:05.123456+05:30"`, `"2024-01-01 21:34:05.123456+00"`},
	}
	// The wire contract requires every one of the 300 zero digits.
	got, err := ConvertValue(cases[0].typ, json.RawMessage(cases[0].raw), "large")
	wantLarge := "1" + strings.Repeat("0", 300)
	if err != nil || string(got) != wantLarge {
		t.Fatalf("large decimal: %s %v", got, err)
	}
	for _, tc := range cases[1:] {
		got, err := ConvertValue(tc.typ, json.RawMessage(tc.raw), "field")
		if err != nil || string(got) != tc.want {
			t.Errorf("%s: got %s %v want %s", tc.typ, got, err, tc.want)
		}
	}
}

func TestDecimalExpansionAllocationBoundary(t *testing.T) {
	// These bounds protect decimal expansion from allocating an unbounded body.
	for _, tc := range []struct{ raw, want string }{
		{"1e" + strconv.Itoa(maxExpandedDigits-1), "1" + strings.Repeat("0", maxExpandedDigits-1)},
		{"1e-" + strconv.Itoa(maxExpandedDigits), "0." + strings.Repeat("0", maxExpandedDigits-1) + "1"},
	} {
		got, err := expandDecimal([]byte(tc.raw))
		if err != nil || got != tc.want {
			t.Fatalf("boundary %s: length=%d err=%v, want length=%d", tc.raw, len(got), err, len(tc.want))
		}
	}
	for _, raw := range []string{"1e" + strconv.Itoa(maxExpandedDigits), "1e-" + strconv.Itoa(maxExpandedDigits+1)} {
		if _, err := expandDecimal([]byte(raw)); err == nil {
			t.Fatalf("accepted expansion beyond allocation limit: %s", raw)
		}
	}
}

func TestConvertValueNumericMatrixPreservesLiterals(t *testing.T) {
	cases := []struct {
		name string
		typ  stream.PGType
		raw  string
		want string
	}{
		{"int2 max", stream.PGInt2, "32767", "32767"},
		{"int2 min", stream.PGInt2, "-32768", "-32768"},
		{"int4 max", stream.PGInt4, "2147483647", "2147483647"},
		{"int8 max", stream.PGInt8, "9223372036854775807", "9223372036854775807"},
		{"int8 min", stream.PGInt8, "-9223372036854775808", "-9223372036854775808"},
		{"float4 ordinary", stream.PGFloat4, "3.14", "3.14"},
		{"float8 ordinary", stream.PGFloat8, "0.1", "0.1"},
		{"float8 negative fraction", stream.PGFloat8, "-1.2300e-2", "-0.012300"},
		{"float8 zero", stream.PGFloat8, "0", "0"},
		{"bool true", stream.PGBool, "true", "true"},
		{"bool false", stream.PGBool, "false", "false"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ConvertValue(tc.typ, json.RawMessage(tc.raw), tc.name)
			if err != nil || string(got) != tc.want {
				t.Fatalf("got %s %v want %s", got, err, tc.want)
			}
		})
	}
}

func TestConvertValueTextJSONAndBinaryMatrix(t *testing.T) {
	cases := []struct {
		name string
		typ  stream.PGType
		raw  string
		want string
	}{
		{"text empty", stream.PGText, `""`, `""`},
		{"text unicode", stream.PGText, `"olá\nworld"`, `"olá\nworld"`},
		{"json object as text", stream.PGJSON, `"{\"b\":2,\"a\":1}"`, `"{\"b\":2,\"a\":1}"`},
		{"bytea ordinary", stream.PGBytea, `"3q2+7w=="`, `"3q2+7w=="`},
		{"bytea raw base64", stream.PGBytea, `"3q2+7w"`, `"3q2+7w"`},
		{"bytea empty", stream.PGBytea, `""`, `""`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ConvertValue(tc.typ, json.RawMessage(tc.raw), tc.name)
			if err != nil || string(got) != tc.want {
				t.Fatalf("got %s %v want %s", got, err, tc.want)
			}
		})
	}
}

func TestConvertValueTimestampMatrix(t *testing.T) {
	cases := []struct {
		typ       stream.PGType
		raw, want string
	}{
		{stream.PGTimestamp, `1704164645000000`, `"2024-01-02 03:04:05"`},
		{stream.PGTimestamp, `1704164645120000`, `"2024-01-02 03:04:05.12"`},
		{stream.PGTimestamp, `1704164645500000`, `"2024-01-02 03:04:05.5"`},
		{stream.PGTimestamptz, `"2024-01-02T03:04:05.123456+05:30"`, `"2024-01-01 21:34:05.123456+00"`},
		{stream.PGTimestamptz, `"2024-01-02T03:04:05Z"`, `"2024-01-02 03:04:05+00"`},
	}
	for _, tc := range cases {
		got, err := ConvertValue(tc.typ, json.RawMessage(tc.raw), "timestamp")
		if err != nil || string(got) != tc.want {
			t.Errorf("%s %s: got %s %v want %s", tc.typ, tc.raw, got, err, tc.want)
		}
	}
}

func TestConvertValueNullIsCanonicalForEveryType(t *testing.T) {
	for _, typ := range []stream.PGType{stream.PGInt2, stream.PGInt4, stream.PGInt8, stream.PGFloat4, stream.PGFloat8, stream.PGBool, stream.PGJSON, stream.PGBytea, stream.PGTimestamp, stream.PGTimestamptz, stream.PGText} {
		got, err := ConvertValue(typ, json.RawMessage(" null "), "nullable")
		if err != nil || string(got) != "null" {
			t.Errorf("%s: got %s %v", typ, got, err)
		}
	}
}

func TestConvertValueErrorsNameFieldWithoutEchoingValue(t *testing.T) {
	const secret = "not-a-valid-secret-value"
	cases := []struct {
		typ stream.PGType
		raw string
	}{
		{stream.PGInt2, `99999`}, {stream.PGInt4, `999999999999`}, {stream.PGInt8, `"` + secret + `"`},
		{stream.PGFloat4, `1e999999999`}, {stream.PGFloat8, `1e999999999`}, {stream.PGBool, `1`},
		{stream.PGJSON, `"not json"`}, {stream.PGBytea, `"!"`}, {stream.PGText, `1`},
		{stream.PGTimestamp, `"bad"`}, {stream.PGTimestamptz, `"bad"`},
	}
	for _, tc := range cases {
		errText := ""
		if _, err := ConvertValue(tc.typ, json.RawMessage(tc.raw), "sensitive_field"); err != nil {
			errText = err.Error()
		}
		if errText == "" || !strings.Contains(errText, `"sensitive_field"`) {
			t.Errorf("%s: error=%q does not identify field", tc.typ, errText)
		}
		if strings.Contains(errText, secret) {
			t.Errorf("%s: error echoed raw value: %q", tc.typ, errText)
		}
	}
}

func TestConvertValueRejectsDecimalExponentOutsideExpansionLimit(t *testing.T) {
	for _, typ := range []stream.PGType{stream.PGFloat4, stream.PGFloat8} {
		for _, raw := range []string{"1e999999999", "1e-999999999", "1e+999999999"} {
			if _, err := ConvertValue(typ, json.RawMessage(raw), "amount"); err == nil {
				t.Errorf("%s accepted exponent %s", typ, raw)
			}
		}
	}
}

func TestConvertValueRejectsMalformed(t *testing.T) {
	for _, tc := range []struct {
		typ stream.PGType
		raw string
	}{
		{stream.PGInt8, `"bad"`}, {stream.PGFloat8, `"bad"`}, {stream.PGBool, `1`},
		{stream.PGJSON, `"bad"`}, {stream.PGBytea, `"!"`}, {stream.PGText, `1`},
		{stream.PGTimestamp, `"bad"`}, {stream.PGTimestamptz, `"bad"`},
	} {
		if _, err := ConvertValue(tc.typ, json.RawMessage(tc.raw), "field"); err == nil {
			t.Errorf("%s accepted %s", tc.typ, tc.raw)
		}
	}
}

func TestAcknowledgements(t *testing.T) {
	cases := []struct {
		body    string
		want    Acknowledgement
		wantErr bool
	}{
		{`{"result":{"success":{}}}`, Success, false},
		{`{"result":{"error":{"policy":"must_retry","class":"c","description":"d"}}}`, Retry, false},
		{`{"result":{"error":{"policy":"keep_going","class":"c","description":"d"}}}`, KeepGoing, false},
		{`{"result":{"success":null}}`, Retry, true},
		{`{"result":{"error":{"policy":"keep_going"}}}`, Retry, true},                             // class/description missing
		{`{"result":{"error":{"policy":"surprise","class":"c","description":"d"}}}`, Retry, true}, // unknown policy
		{`{"result":{"success":{},"error":{"policy":"must_retry","class":"c","description":"d"}}}`, Success, false},
		{`{"result":{}}`, Retry, true},
		{``, Retry, true},
		{`{"result":[]}`, Retry, true},
	}
	for _, tc := range cases {
		got, err := DecodeAcknowledgement([]byte(tc.body))
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Fatalf("%s: got %v %v (wantErr=%v)", tc.body, got, err, tc.wantErr)
		}
	}
}

// FuzzDecodeAcknowledgement checks that DecodeAcknowledgement never panics,
// that a returned error always implies Retry, and that a Success result
// always decodes a non-nil result.success object.
func FuzzDecodeAcknowledgement(f *testing.F) {
	// Seeds: the bodies in the TestAcknowledgements rows.
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

func TestMatchesFilterFailsOpen(t *testing.T) {
	f := &Filter{Column: "event_name", Values: []string{"Created"}}
	for _, tc := range []struct {
		payload string
		want    bool
	}{
		{`{"event_name":"Created"}`, true}, {`{"event_name":"Deleted"}`, false},
		{`{"other":1}`, true}, {`{"event_name":3}`, true}, {`"text"`, true}, {`{bad`, true},
	} {
		if got := MatchesFilter(f, []byte(tc.payload)); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.payload, got, tc.want)
		}
	}
	if !MatchesFilter(nil, []byte(`{bad`)) {
		t.Fatal("nil filter must match")
	}
}

func TestFilterFailsOpen(t *testing.T) {
	f := &Filter{Column: "event_name", Values: []string{"Created"}}
	if !MatchesFilter(f, []byte(`{"other":1}`)) || !MatchesFilter(f, []byte(`{"event_name":3}`)) || MatchesFilter(f, []byte(`{"event_name":"Deleted"}`)) {
		t.Fatal("unexpected filter behavior")
	}
}

func TestFilterMatrix(t *testing.T) {
	filter := &Filter{Column: "event_name", Values: []string{"Created"}}
	cases := []struct {
		name, payload string
		want          bool
	}{
		{"no filter", `{"event_name":"Deleted"}`, true}, {"matching string", `{"event_name":"Created"}`, true},
		{"non-matching string", `{"event_name":"Deleted"}`, false}, {"missing column", `{"other":1}`, true},
		{"non-string value", `{"event_name":3}`, true}, {"non-object payload", `"just text"`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := MatchesFilter(func() *Filter {
				if tc.name == "no filter" {
					return nil
				}
				return filter
			}(), []byte(tc.payload)); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

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
