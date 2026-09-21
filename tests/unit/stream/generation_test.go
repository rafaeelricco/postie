package stream_test

import (
	"testing"

	"github.com/rafaeelricco/postie/internal/stream"
)

func TestGenerationBoundaryAndString(t *testing.T) {
	for _, tc := range []struct {
		generation stream.Generation
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
