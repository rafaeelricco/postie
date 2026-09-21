package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/config"
	"github.com/rafaeelricco/postie/internal/stream"
	"gopkg.in/yaml.v3"
)

func writeEngineConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "postie.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write engine config: %v", err)
	}
	return path
}

const baseEngineConfig = `
version: 1
namespace: ns
environment: development
kafka:
  brokers: [kafka:9092]
connect:
  url: http://connect:8083
operator:
  listen: 0.0.0.0:8081
control:
  database_url: postgres://control
`

func TestApplicationValidationPreservesFilterBehavior(t *testing.T) {
	c := config.Application{Sources: []config.Source{{ID: "events", Description: "events", Type: "postgres", Host: "db", Port: 5432, Username: "u", Database: "d", Table: "events", Columns: []string{"id", "correlation_id"}, SerialColumn: "id", PartitioningColumn: "correlation_id"}}, Destinations: []config.Destination{{ID: "projection", Description: "projection", Type: "http-push", Endpoint: "http://example.test", Username: "u", Sources: []string{"events"}, Filter: &config.Filter{Column: "event_name", Values: []string{"Created"}}}}}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestApplicationValidationRejectsUnknownSource(t *testing.T) {
	c := config.Application{Sources: []config.Source{{ID: "events", Description: "events", Type: "postgres", Host: "db", Port: 1, Username: "u", Database: "d", Table: "events", Columns: []string{"id", "key"}, SerialColumn: "id", PartitioningColumn: "key"}}, Destinations: []config.Destination{{ID: "d", Description: "d", Type: "http-push", Endpoint: "http://e", Username: "u", Sources: []string{"missing"}}}}
	if c.Validate() == nil {
		t.Fatal("expected error")
	}
}

func TestFixtures(t *testing.T) {
	t.Setenv("SOURCE_PASSWORD", "src-pass")
	t.Setenv("DEST_PASSWORD", "dest-pass")

	cases := []struct {
		file    string
		wantErr string // empty means Validate must succeed
	}{
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

	for _, tc := range cases {
		t.Run(tc.file, func(t *testing.T) {
			path := filepath.Join("..", "..", "fixtures", "config", tc.file)
			_, err := config.LoadApplication(path)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected %s to be valid, got error: %v", tc.file, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected %s to fail validation with an error containing %q, got nil", tc.file, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected %s error to contain %q, got %q", tc.file, tc.wantErr, err.Error())
			}
		})
	}
}

func TestExpandEnvironmentMissing(t *testing.T) {
	const missing = "DEFINITELY_UNSET_CONFIG_VAR"
	_, err := config.ExpandEnvironment("password: ${"+missing+"}", os.LookupEnv)
	if err == nil {
		t.Fatal("expected an error for a missing environment variable")
	}
	if !strings.Contains(err.Error(), missing) {
		t.Fatalf("expected error to name %q, got %q", missing, err.Error())
	}
}

func TestExpandEnvironmentReportsAllMissing(t *testing.T) {
	lookup := func(name string) (string, bool) {
		if name == "SET" {
			return "value", true
		}
		return "", false
	}
	_, err := config.ExpandEnvironment("${B} ${A} ${B} ${SET}", lookup)
	if err == nil {
		t.Fatal("expected an error for missing environment variables")
	}
	if !strings.Contains(err.Error(), "are not set") {
		t.Fatalf("expected error to say \"are not set\", got %q", err.Error())
	}
	indexA := strings.Index(err.Error(), "A")
	indexB := strings.Index(err.Error(), "B")
	if indexA == -1 || indexB == -1 || indexA > indexB {
		t.Fatalf("expected error to name A and B once each, A before B, got %q", err.Error())
	}
	if strings.Contains(err.Error(), "SET") {
		t.Fatalf("expected error not to mention the set variable, got %q", err.Error())
	}
}

func TestExpandEnvironmentIsPure(t *testing.T) {
	values := map[string]string{"FAKE_ONLY": "fake-value"}
	lookup := func(name string) (string, bool) {
		v, ok := values[name]
		return v, ok
	}
	out, err := config.ExpandEnvironment("${FAKE_ONLY}", lookup)
	if err != nil {
		t.Fatalf("expected expansion to succeed, got error: %v", err)
	}
	if out != "fake-value" {
		t.Fatalf("expected %q, got %q", "fake-value", out)
	}

	// FAKE_ONLY must never be a real process environment variable, so this
	// call proves ExpandEnvironment never consulted os.Getenv/os.LookupEnv.
	if _, ok := os.LookupEnv("FAKE_ONLY"); ok {
		t.Fatal("test setup invalid: FAKE_ONLY must not be set in the process environment")
	}
}

func TestRedactedHidesPasswords(t *testing.T) {
	const sourcePassword = "super-secret-source-password"
	const destPassword = "super-secret-destination-password"

	original := config.Application{
		Sources: []config.Source{{
			ID: "s1", Description: "source one", Type: "postgres",
			Host: "db", Port: 5432, Username: "u", Password: sourcePassword,
			Database: "d", Table: "events", Columns: []string{"id", "key"},
			SerialColumn: "id", PartitioningColumn: "key",
		}},
		Destinations: []config.Destination{{
			ID: "d1", Description: "destination one", Type: "http-push",
			Endpoint: "http://example.test", Username: "u", Password: destPassword,
			Sources: []string{"s1"},
			Filter:  &config.Filter{Column: "event_name", Values: []string{"Created"}},
		}},
	}

	redacted := original.Redacted()

	if redacted.Sources[0].Password != "[REDACTED]" {
		t.Fatalf("expected source password to be redacted, got %q", redacted.Sources[0].Password)
	}
	if redacted.Destinations[0].Password != "[REDACTED]" {
		t.Fatalf("expected destination password to be redacted, got %q", redacted.Destinations[0].Password)
	}

	out, err := yaml.Marshal(redacted)
	if err != nil {
		t.Fatalf("marshal redacted config: %v", err)
	}
	if strings.Contains(string(out), sourcePassword) || strings.Contains(string(out), destPassword) {
		t.Fatalf("redacted yaml still contains a real password:\n%s", out)
	}

	// Mutating the redacted copy's slices must not reach back into the original:
	// Redacted must not alias the source Application's backing arrays.
	redacted.Sources[0].Columns[0] = "mutated"
	redacted.Destinations[0].Sources[0] = "mutated"
	redacted.Destinations[0].Filter.Values[0] = "mutated"

	if original.Sources[0].Password != sourcePassword {
		t.Fatal("Redacted mutated the original source's password")
	}
	if original.Sources[0].Columns[0] != "id" {
		t.Fatal("Redacted aliased the original source's Columns slice")
	}
	if original.Destinations[0].Sources[0] != "s1" {
		t.Fatal("Redacted aliased the original destination's Sources slice")
	}
	if original.Destinations[0].Filter.Values[0] != "Created" {
		t.Fatal("Redacted aliased the original destination's Filter.Values slice")
	}
}

func TestMultiDestinationConfigLoads(t *testing.T) {
	t.Setenv("POSTIE_CAPTURE_PASSWORD", "capture-pass")
	t.Setenv("POSTIE_DELIVERY_PASSWORD", "delivery-pass")

	path := filepath.Join("..", "..", "fixtures", "config", "multi-destination.yaml")
	app, err := config.LoadApplication(path)
	if err != nil {
		t.Fatalf("expected multi-destination.yaml to load, got error: %v", err)
	}
	if len(app.Sources) != 1 {
		t.Fatalf("expected 1 source, got %d", len(app.Sources))
	}
	if len(app.Destinations) != 58 {
		t.Fatalf("expected 58 destinations, got %d", len(app.Destinations))
	}
}

func TestLoadEngineDefaults(t *testing.T) {
	e, err := config.LoadEngine(writeEngineConfig(t, baseEngineConfig))
	if err != nil {
		t.Fatalf("expected base config to load, got error: %v", err)
	}
	if e.Kafka.Partitions != 10 {
		t.Fatalf("expected default partitions 10, got %d", e.Kafka.Partitions)
	}
	if e.Kafka.ReplicationFactor != 1 {
		t.Fatalf("expected default replication_factor 1, got %d", e.Kafka.ReplicationFactor)
	}
	if e.Delivery.RequestTimeout != 60*time.Second {
		t.Fatalf("expected default request_timeout 60s, got %s", e.Delivery.RequestTimeout)
	}
	if e.Delivery.DrainTimeout != 30*time.Second {
		t.Fatalf("expected default drain_timeout 30s, got %s", e.Delivery.DrainTimeout)
	}
}

func TestLoadEngineProductionRequiresReplicationThree(t *testing.T) {
	production := `
version: 1
namespace: ns
environment: production
kafka:
  brokers: [kafka:9092]
connect:
  url: http://connect:8083
operator:
  listen: 0.0.0.0:8081
control:
  database_url: postgres://control
`
	if _, err := config.LoadEngine(writeEngineConfig(t, production)); err == nil {
		t.Fatal("expected production config without replication_factor: 3 to fail")
	} else if !strings.Contains(err.Error(), "replication_factor must be 3") {
		t.Fatalf("expected error to mention replication_factor, got %q", err.Error())
	}

	withThreeConfig := `
version: 1
namespace: ns
environment: production
kafka:
  brokers: [kafka:9092]
  replication_factor: 3
connect:
  url: http://connect:8083
operator:
  listen: 0.0.0.0:8081
control:
  database_url: postgres://control
`
	if _, err := config.LoadEngine(writeEngineConfig(t, withThreeConfig)); err != nil {
		t.Fatalf("expected production config with replication_factor: 3 to load, got error: %v", err)
	}
}

func TestLoadEngineRequiresControlDatabase(t *testing.T) {
	withoutControl := `
version: 1
namespace: ns
environment: development
kafka:
  brokers: [kafka:9092]
connect:
  url: http://connect:8083
operator:
  listen: 0.0.0.0:8081
`
	_, err := config.LoadEngine(writeEngineConfig(t, withoutControl))
	if err == nil {
		t.Fatal("expected missing control.database_url to fail")
	}
	if !strings.Contains(err.Error(), "control.database_url is required") {
		t.Fatalf("expected error to mention control.database_url, got %q", err.Error())
	}
}

func TestLoadEngineRejectsUnknownKind(t *testing.T) {
	cfg := baseEngineConfig + `destinations:
  Accounts_Projection:
    kind: bogus
`
	_, err := config.LoadEngine(writeEngineConfig(t, cfg))
	if err == nil {
		t.Fatal("expected unknown destination kind to fail")
	}
	if !strings.Contains(err.Error(), `kind must be "projection" or "reaction"`) {
		t.Fatalf("expected error to name the allowed kinds, got %q", err.Error())
	}
}

func TestLoadEngineRejectsReactionReplayEndpoint(t *testing.T) {
	cfg := baseEngineConfig + `destinations:
  Notifications_Reaction:
    kind: reaction
    replay_endpoint: http://receiver:8080/rebuild/reactions/notifications
`
	_, err := config.LoadEngine(writeEngineConfig(t, cfg))
	if err == nil {
		t.Fatal("expected a reaction destination with replay_endpoint to fail")
	}
	if !strings.Contains(err.Error(), "reaction destinations must not set replay_endpoint") {
		t.Fatalf("expected error to mention reaction replay_endpoint, got %q", err.Error())
	}
}

func TestPolicyForDefaultsToReaction(t *testing.T) {
	e, err := config.LoadEngine(writeEngineConfig(t, baseEngineConfig+`destinations:
  Accounts_Projection:
    kind: projection
    replay_endpoint: http://receiver:8080/rebuild/projections/accounts
`))
	if err != nil {
		t.Fatalf("expected config to load, got error: %v", err)
	}

	if got := e.PolicyFor("Accounts_Projection"); got != (config.DestinationPolicy{Kind: "projection", ReplayEndpoint: "http://receiver:8080/rebuild/projections/accounts"}) {
		t.Fatalf("expected listed destination's policy, got %+v", got)
	}
	if got := e.PolicyFor("Unlisted_Reaction"); got != (config.DestinationPolicy{Kind: "reaction"}) {
		t.Fatalf("expected unlisted destination to default to reaction, got %+v", got)
	}
}

func TestExampleEngineConfigLoads(t *testing.T) {
	t.Setenv("POSTIE_DATABASE_URL", "postgres://control-store")

	// application_config in examples/postie.yaml is a path relative to the
	// repo root; LoadEngine never reads it, so only LoadEngine is exercised.
	path := filepath.Join("..", "..", "..", "examples", "postie.yaml")
	e, err := config.LoadEngine(path)
	if err != nil {
		t.Fatalf("expected examples/postie.yaml to load, got error: %v", err)
	}

	if e.Kafka.ReplicationFactor != 1 {
		t.Fatalf("expected replication_factor 1, got %d", e.Kafka.ReplicationFactor)
	}
	if e.Control.DatabaseURL != "postgres://control-store" {
		t.Fatalf("expected control.database_url to expand, got %q", e.Control.DatabaseURL)
	}
	if e.Delivery.RequestTimeout != 60*time.Second || e.Delivery.DrainTimeout != 30*time.Second {
		t.Fatalf("expected delivery timeouts 60s/30s, got %s/%s", e.Delivery.RequestTimeout, e.Delivery.DrainTimeout)
	}
	want := config.DestinationPolicy{Kind: "projection", ReplayEndpoint: "http://receiver:8080/rebuild/projections/accounts"}
	if got := e.PolicyFor("Accounts_Projection"); got != want {
		t.Fatalf("expected Accounts_Projection policy %+v, got %+v", want, got)
	}
}

func TestLoadEngineRejectsPartitionOverflow(t *testing.T) {
	cfg := `
version: 1
namespace: ns
environment: development
kafka:
  brokers: [kafka:9092]
  partitions: 3000000000
connect:
  url: http://connect:8083
operator:
  listen: 0.0.0.0:8081
control:
  database_url: postgres://control
`
	if _, err := config.LoadEngine(writeEngineConfig(t, cfg)); err == nil {
		t.Fatal("expected a partitions value overflowing int32 to fail")
	}
}

func TestLoadEngineRejectsNonPositiveReplication(t *testing.T) {
	cfg := `
version: 1
namespace: ns
environment: development
kafka:
  brokers: [kafka:9092]
  replication_factor: -1
connect:
  url: http://connect:8083
operator:
  listen: 0.0.0.0:8081
control:
  database_url: postgres://control
`
	_, err := config.LoadEngine(writeEngineConfig(t, cfg))
	if err == nil {
		t.Fatal("expected a non-positive replication_factor to fail")
	}
	if !strings.Contains(err.Error(), "replication_factor must be positive") {
		t.Fatalf("expected error to mention replication_factor must be positive, got %q", err.Error())
	}
}

func TestSourcePolicyPublicationLoads(t *testing.T) {
	cfg := baseEngineConfig + `sources:
  events:
    publication: events_pub
`
	e, err := config.LoadEngine(writeEngineConfig(t, cfg))
	if err != nil {
		t.Fatalf("expected config to load, got error: %v", err)
	}
	if got := e.Sources["events"].Publication; got != "events_pub" {
		t.Fatalf("expected publication %q, got %q", "events_pub", got)
	}
}

func TestGenerationValid(t *testing.T) {
	if stream.Generation(0).Valid() {
		t.Fatal("expected generation 0 to be invalid")
	}
	if stream.Generation(-1).Valid() {
		t.Fatal("expected generation -1 to be invalid")
	}
	if !stream.Generation(1).Valid() {
		t.Fatal("expected generation 1 to be valid")
	}
	if got := stream.Generation(1).String(); got != "1" {
		t.Fatalf("expected String() == %q, got %q", "1", got)
	}
}
