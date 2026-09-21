package stream_test

import (
	"testing"

	"github.com/rafaeelricco/postie/internal/stream"
)

func TestPGTypeSupportedAndInteger(t *testing.T) {
	for _, tc := range []struct {
		typ                stream.PGType
		supported, integer bool
	}{
		{stream.PGInt2, true, true}, {stream.PGInt4, true, true}, {stream.PGInt8, true, true},
		{stream.PGFloat4, true, false}, {stream.PGFloat8, true, false}, {stream.PGBool, true, false},
		{stream.PGJSON, true, false}, {stream.PGBytea, true, false}, {stream.PGTimestamp, true, false},
		{stream.PGTimestamptz, true, false}, {stream.PGText, true, false},
		{stream.PGType("uuid"), false, false}, {stream.PGType(""), false, false},
	} {
		if got := tc.typ.Supported(); got != tc.supported {
			t.Errorf("PGType(%q).Supported() = %v, want %v", tc.typ, got, tc.supported)
		}
		if got := tc.typ.Integer(); got != tc.integer {
			t.Errorf("PGType(%q).Integer() = %v, want %v", tc.typ, got, tc.integer)
		}
	}
}
