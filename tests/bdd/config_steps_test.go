package bdd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cucumber/godog"
	"gopkg.in/yaml.v3"

	"github.com/rafaeelricco/postie/internal/config"
)

// configWorld is rebuilt in sc.Before for every scenario; the environment
// variables it sets are cleared in both Before and After so scenarios never
// see a password left over by a previous one.
type configWorld struct {
	path string
	app  config.Application
	err  error
}

func registerConfigSteps(sc *godog.ScenarioContext) {
	w := &configWorld{}

	clearEnv := func() {
		os.Unsetenv("SOURCE_PASSWORD")
		os.Unsetenv("DEST_PASSWORD")
	}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		*w = configWorld{}
		clearEnv()
		return ctx, nil
	})
	sc.After(func(ctx context.Context, _ *godog.Scenario, err error) (context.Context, error) {
		clearEnv()
		return ctx, err
	})

	sc.Given(`^SOURCE_PASSWORD and DEST_PASSWORD are set$`, w.bothPasswordsSet)
	sc.Given(`^DEST_PASSWORD is set$`, w.destPasswordSet)
	sc.Given(`^SOURCE_PASSWORD is not set$`, w.sourcePasswordNotSet)

	sc.When(`^I load the application config "([^"]*)"$`, w.loadConfig)

	sc.Then(`^it is accepted$`, w.itIsAccepted)
	sc.Then(`^it is rejected with "([^"]*)"$`, w.itIsRejectedWith)
	sc.Then(`^the redacted config does not contain "([^"]*)"$`, w.redactedDoesNotContain)
}

func (w *configWorld) bothPasswordsSet() error {
	os.Setenv("SOURCE_PASSWORD", "src-pass")
	os.Setenv("DEST_PASSWORD", "dest-pass")
	return nil
}

func (w *configWorld) destPasswordSet() error {
	os.Setenv("DEST_PASSWORD", "dest-pass")
	return nil
}

func (w *configWorld) sourcePasswordNotSet() error {
	os.Unsetenv("SOURCE_PASSWORD")
	return nil
}

// loadConfig loads a fixture by name. The path is relative to tests/bdd,
// where TestFeatures runs, so it climbs to ../fixtures/config.
func (w *configWorld) loadConfig(file string) error {
	w.path = filepath.Join("..", "fixtures", "config", file)
	w.app, w.err = config.LoadApplication(w.path)
	return nil
}

func (w *configWorld) itIsAccepted() error {
	if w.err != nil {
		return fmt.Errorf("expected %s to be accepted, got error: %w", w.path, w.err)
	}
	return nil
}

func (w *configWorld) itIsRejectedWith(want string) error {
	if w.err == nil {
		return fmt.Errorf("expected %s to be rejected with %q, got no error", w.path, want)
	}
	if !strings.Contains(w.err.Error(), want) {
		return fmt.Errorf("expected error to contain %q, got %q", want, w.err.Error())
	}
	return nil
}

func (w *configWorld) redactedDoesNotContain(secret string) error {
	out, err := yaml.Marshal(w.app.Redacted())
	if err != nil {
		return err
	}
	if strings.Contains(string(out), secret) {
		return fmt.Errorf("redacted config still contains %q:\n%s", secret, out)
	}
	return nil
}
