package config_test

import (
	"strings"
	"testing"

	"github.com/rafaeelricco/postie/internal/config"
)

// Mutation testing showed no test sat on either side of these two limits.

func TestLoadEnginePartitionBoundary(t *testing.T) {
	withPartitions := func(n string) string {
		return strings.Replace(baseEngineConfig, "brokers: [kafka:9092]", "brokers: [kafka:9092]\n  partitions: "+n, 1)
	}
	e, err := config.LoadEngine(writeEngineConfig(t, withPartitions("1")))
	if err != nil || e.Kafka.Partitions != 1 {
		t.Fatalf("expected a single partition to be accepted, got %d, %v", e.Kafka.Partitions, err)
	}
	if _, err := config.LoadEngine(writeEngineConfig(t, withPartitions("-1"))); err == nil {
		t.Fatal("expected negative partitions to be rejected")
	}
}

func TestSourcePortBoundary(t *testing.T) {
	app := func(port int) config.Application {
		return config.Application{Sources: []config.Source{{ID: "s", Description: "s", Type: config.SourcePostgres, Host: "h", Port: port, Username: "u", Database: "d", Table: "t", Columns: []string{"id", "k"}, SerialColumn: "id", PartitioningColumn: "k"}}}
	}
	if err := app(1).Validate(); err != nil {
		t.Fatalf("expected port 1 to be accepted, got %v", err)
	}
	if err := app(0).Validate(); err == nil || !strings.Contains(err.Error(), "incomplete PostgreSQL connection") {
		t.Fatalf("expected port 0 to be rejected as incomplete, got %v", err)
	}
}
