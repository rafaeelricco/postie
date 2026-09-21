package config_test

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/rafaeelricco/postie/internal/config"
)

// multiDestinationEngineConfig builds an engine config whose
// application_config is the absolute path of
// tests/fixtures/config/multi-destination.yaml, so config.Load can read both
// halves regardless of the test binary's working directory.
func multiDestinationEngineConfig(t *testing.T) string {
	t.Helper()
	appPath, err := filepath.Abs(filepath.Join("..", "..", "fixtures", "config", "multi-destination.yaml"))
	if err != nil {
		t.Fatalf("resolve application config path: %v", err)
	}
	return baseEngineConfig + "application_config: " + appPath + "\n"
}

func TestLoadReadsEngineThenApplication(t *testing.T) {
	t.Setenv("POSTIE_CAPTURE_PASSWORD", "capture-pass")
	t.Setenv("POSTIE_DELIVERY_PASSWORD", "delivery-pass")

	engine, application, err := config.Load(writeEngineConfig(t, multiDestinationEngineConfig(t)))
	if err != nil {
		t.Fatalf("expected config to load, got error: %v", err)
	}
	if engine.Kafka.Partitions != 10 {
		t.Fatalf("expected default partitions 10, got %d", engine.Kafka.Partitions)
	}
	if len(application.Sources) != 1 {
		t.Fatalf("expected 1 source, got %d", len(application.Sources))
	}
	if len(application.Destinations) != 58 {
		t.Fatalf("expected 58 destinations, got %d", len(application.Destinations))
	}
}

func TestLoadStopsAtEngineError(t *testing.T) {
	engine, application, err := config.Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err == nil {
		t.Fatal("expected a missing engine config to fail")
	}
	if !reflect.DeepEqual(engine, config.Engine{}) {
		t.Fatalf("expected a zero-valued Engine, got %+v", engine)
	}
	if !reflect.DeepEqual(application, config.Application{}) {
		t.Fatalf("expected a zero-valued Application, got %+v", application)
	}
}
