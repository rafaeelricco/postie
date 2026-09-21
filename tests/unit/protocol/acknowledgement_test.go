package protocol_test

import (
	"testing"

	"github.com/rafaeelricco/postie/internal/protocol"
)

func TestAcknowledgements(t *testing.T) {
	cases := []struct {
		body    string
		want    protocol.Acknowledgement
		wantErr bool
	}{
		{`{"result":{"success":{}}}`, protocol.Success, false},
		{`{"result":{"error":{"policy":"must_retry","class":"c","description":"d"}}}`, protocol.Retry, false},
		{`{"result":{"error":{"policy":"keep_going","class":"c","description":"d"}}}`, protocol.KeepGoing, false},
		{`{"result":{"success":null}}`, protocol.Retry, true},
		{`{"result":{"error":{"policy":"keep_going"}}}`, protocol.Retry, true},                             // class/description missing
		{`{"result":{"error":{"policy":"surprise","class":"c","description":"d"}}}`, protocol.Retry, true}, // unknown policy
		{`{"result":{"success":{},"error":{"policy":"must_retry","class":"c","description":"d"}}}`, protocol.Success, false},
		{`{"result":{}}`, protocol.Retry, true},
		{``, protocol.Retry, true},
	}
	for _, tc := range cases {
		got, err := protocol.DecodeAcknowledgement([]byte(tc.body))
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Fatalf("%s: got %v %v (wantErr=%v)", tc.body, got, err, tc.wantErr)
		}
	}
}
