package protocol

import "testing"

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
	}
	for _, tc := range cases {
		got, err := DecodeAcknowledgement([]byte(tc.body))
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Fatalf("%s: got %v %v (wantErr=%v)", tc.body, got, err, tc.wantErr)
		}
	}
}
