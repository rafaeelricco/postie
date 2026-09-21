// Package bdd holds the user-visible behavior scenarios. Each scenario is a
// subtest that builds its own world and calls that world's steps in order.
package bdd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/rafaeelricco/postie/internal/config"
)

// configWorld is built per scenario by newConfigWorld, which starts with both
// password variables unset and lets t.Setenv restore them afterwards.
type configWorld struct {
	t    *testing.T
	path string
	app  config.Application
	err  error
}

func newConfigWorld(t *testing.T) *configWorld {
	t.Helper()
	// ExpandEnvironment tests presence, not emptiness, so the variables must be
	// truly unset. t.Setenv registers the restore; Unsetenv does the clearing.
	for _, name := range []string{"SOURCE_PASSWORD", "DEST_PASSWORD"} {
		t.Setenv(name, "")
		os.Unsetenv(name)
	}
	return &configWorld{t: t}
}

// docs/specification.md, "Configuration": validation, environment substitution, redaction.
func TestConfig(t *testing.T) {
	t.Run("fixture configs are accepted or rejected", func(t *testing.T) {
		cases := []struct{ file, rejectedWith string }{
			{"valid-full.yaml", ""},
			{"valid-filter.yaml", ""},
			{"valid-no-filter.yaml", ""},
			{"invalid-empty-filter-values.yaml", "filter column and values are required"},
			{"invalid-duplicate-source.yaml", "duplicate source id"},
			{"invalid-duplicate-destination.yaml", "duplicate destination id"},
			{"invalid-unknown-source.yaml", "unknown source"},
			{"invalid-unsupported-source-type.yaml", "unsupported type"},
			{"invalid-unsupported-destination-type.yaml", "unsupported type"},
			{"invalid-columns-omit-partitioning.yaml", "columns must include serialColumn and partitioningColumn"},
		}
		for _, c := range cases {
			t.Run(c.file, func(t *testing.T) {
				w := newConfigWorld(t)
				w.bothPasswordsSet()
				w.loadConfig(c.file)
				if c.rejectedWith == "" {
					w.itIsAccepted()
					return
				}
				w.itIsRejectedWith(c.rejectedWith)
			})
		}
	})

	t.Run("a missing environment variable fails the load", func(t *testing.T) {
		w := newConfigWorld(t)
		w.destPasswordSet()
		w.sourcePasswordNotSet()
		w.loadConfig("valid-full.yaml")
		w.itIsRejectedWith("environment variable SOURCE_PASSWORD is not set")
	})

	t.Run("redacted configuration never shows a password", func(t *testing.T) {
		w := newConfigWorld(t)
		w.bothPasswordsSet()
		w.loadConfig("valid-full.yaml")
		w.itIsAccepted()
		w.redactedDoesNotContain("src-pass")
		w.redactedDoesNotContain("dest-pass")
	})
}

func (w *configWorld) bothPasswordsSet() {
	w.t.Setenv("SOURCE_PASSWORD", "src-pass")
	w.t.Setenv("DEST_PASSWORD", "dest-pass")
}

func (w *configWorld) destPasswordSet() {
	w.t.Setenv("DEST_PASSWORD", "dest-pass")
}

func (w *configWorld) sourcePasswordNotSet() {
	os.Unsetenv("SOURCE_PASSWORD")
}

// loadConfig loads a fixture by name. The path is relative to tests/bdd,
// where TestConfig runs, so it climbs to ../fixtures/config.
func (w *configWorld) loadConfig(file string) {
	w.path = filepath.Join("..", "fixtures", "config", file)
	w.app, w.err = config.LoadApplication(w.path)
}

func (w *configWorld) itIsAccepted() {
	w.t.Helper()
	if w.err != nil {
		w.t.Fatalf("expected %s to be accepted, got error: %v", w.path, w.err)
	}
}

func (w *configWorld) itIsRejectedWith(want string) {
	w.t.Helper()
	if w.err == nil {
		w.t.Fatalf("expected %s to be rejected with %q, got no error", w.path, want)
	}
	if !strings.Contains(w.err.Error(), want) {
		w.t.Fatalf("expected error to contain %q, got %q", want, w.err.Error())
	}
}

func (w *configWorld) redactedDoesNotContain(secret string) {
	w.t.Helper()
	out, err := yaml.Marshal(w.app.Redacted())
	if err != nil {
		w.t.Fatal(err)
	}
	if strings.Contains(string(out), secret) {
		w.t.Fatalf("redacted config still contains %q:\n%s", secret, out)
	}
}
