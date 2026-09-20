package protocol

import "testing"

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
