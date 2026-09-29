package stream

import "testing"

func TestGenerationBoundaryAndString(t *testing.T) {
	for _, tc := range []struct {
		generation Generation
		valid      bool
		text       string
	}{
		{-1, false, "-1"},
		{0, false, "0"},
		{1, true, "1"},
		{23, true, "23"},
	} {
		if got := tc.generation.Valid(); got != tc.valid {
			t.Errorf("Generation(%d).Valid() = %v, want %v", tc.generation, got, tc.valid)
		}
		if got := tc.generation.String(); got != tc.text {
			t.Errorf("Generation(%d).String() = %q, want %q", tc.generation, got, tc.text)
		}
	}
}

func TestPGTypeSupportedAndInteger(t *testing.T) {
	for _, tc := range []struct {
		typ                PGType
		supported, integer bool
	}{
		{PGInt2, true, true}, {PGInt4, true, true}, {PGInt8, true, true},
		{PGFloat4, true, false}, {PGFloat8, true, false}, {PGBool, true, false},
		{PGJSON, true, false}, {PGBytea, true, false}, {PGTimestamp, true, false},
		{PGTimestamptz, true, false}, {PGText, true, false},
		{PGType("uuid"), false, false}, {PGType(""), false, false},
	} {
		if got := tc.typ.Supported(); got != tc.supported {
			t.Errorf("PGType(%q).Supported() = %v, want %v", tc.typ, got, tc.supported)
		}
		if got := tc.typ.Integer(); got != tc.integer {
			t.Errorf("PGType(%q).Integer() = %v, want %v", tc.typ, got, tc.integer)
		}
	}
}
