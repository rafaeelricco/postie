package config

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

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
	c := Application{Sources: []Source{{ID: "events", Description: "events", Type: "postgres", Host: "db", Port: 5432, Username: "u", Database: "d", Table: "events", Columns: []string{"id", "correlation_id"}, SerialColumn: "id", PartitioningColumn: "correlation_id"}}, Destinations: []Destination{{ID: "projection", Description: "projection", Type: "http-push", Endpoint: "http://example.test", Username: "u", Sources: []string{"events"}, Filter: &Filter{Column: "event_name", Values: []string{"Created"}}}}}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestApplicationValidationRejectsUnknownSource(t *testing.T) {
	c := Application{Sources: []Source{{ID: "events", Description: "events", Type: "postgres", Host: "db", Port: 1, Username: "u", Database: "d", Table: "events", Columns: []string{"id", "key"}, SerialColumn: "id", PartitioningColumn: "key"}}, Destinations: []Destination{{ID: "d", Description: "d", Type: "http-push", Endpoint: "http://e", Username: "u", Sources: []string{"missing"}}}}
	if c.Validate() == nil {
		t.Fatal("expected error")
	}
}

// validApplication is the application config the loading tests start from;
// each invalid case changes one line of it.
const validApplication = `
data_sources:
  - id: postgres_source
    description: Main events store
    type: postgres
    host: localhost
    port: 5432
    username: my_user
    password: ${SOURCE_PASSWORD}
    database: my_db
    table: events_table
    columns:
      - id
      - aggregate_id
      - sequence_number
      - payload
    serialColumn: id
    partitioningColumn: aggregate_id

  - id: secondary_source
    description: Secondary events store
    type: postgres
    host: localhost
    port: 5433
    username: my_other_user
    password: ${SOURCE_PASSWORD}
    database: my_other_db
    table: other_events_table
    columns:
      - id
      - correlation_id
      - payload
    serialColumn: id
    partitioningColumn: correlation_id

data_destinations:
  - id: HTTP_destination
    description: my projection 2
    type: http-push
    endpoint: http://receiver.example.invalid:8080/my_projection
    username: name-of-user
    password: ${DEST_PASSWORD}
    sources:
      - postgres_source
      - secondary_source

  - id: filtered_destination
    description: filtered projection
    type: http-push
    endpoint: http://receiver.example.invalid:8080/filtered_projection
    username: name-of-user
    password: ${DEST_PASSWORD}
    sources:
      - postgres_source
    filter:
      column: event_name
      values:
        - OrderCreated
        - OrderUpdated
`

func TestApplicationFixtures(t *testing.T) {
	t.Setenv("SOURCE_PASSWORD", "src-pass")
	t.Setenv("DEST_PASSWORD", "dest-pass")

	for _, tc := range []struct{ name, old, new, wantErr string }{
		{"valid", "", "", ""},
		{"empty filter values", "      values:\n        - OrderCreated\n        - OrderUpdated", "      values: []", "filter column and values are required"},
		{"duplicate source", "id: secondary_source", "id: postgres_source", "duplicate source id"},
		{"duplicate destination", "id: filtered_destination", "id: HTTP_destination", "duplicate destination id"},
		{"unknown source", "      - secondary_source\n", "      - missing_source\n", "unknown source"},
		{"unsupported source type", "type: postgres", "type: mysql", "unsupported type"},
		{"unsupported destination type", "type: http-push", "type: file", "unsupported type"},
		{"columns omit partitioning", "      - aggregate_id\n", "", "columns must include serialColumn and partitioningColumn"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadApplication(writeEngineConfig(t, strings.Replace(validApplication, tc.old, tc.new, 1)))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected %s to be valid, got error: %v", tc.name, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected %s to fail validation with an error containing %q, got nil", tc.name, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected %s error to contain %q, got %q", tc.name, tc.wantErr, err.Error())
			}
		})
	}
}

func TestExpandEnvironmentMissing(t *testing.T) {
	const missing = "DEFINITELY_UNSET_CONFIG_VAR"
	_, err := ExpandEnvironment("password: ${"+missing+"}", os.LookupEnv)
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
	_, err := ExpandEnvironment("${B} ${A} ${B} ${SET}", lookup)
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
	out, err := ExpandEnvironment("${FAKE_ONLY}", lookup)
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

	original := Application{
		Sources: []Source{{
			ID: "s1", Description: "source one", Type: "postgres",
			Host: "db", Port: 5432, Username: "u", Password: sourcePassword,
			Database: "d", Table: "events", Columns: []string{"id", "key"},
			SerialColumn: "id", PartitioningColumn: "key",
		}},
		Destinations: []Destination{{
			ID: "d1", Description: "destination one", Type: "http-push",
			Endpoint: "http://example.test", Username: "u", Password: destPassword,
			Sources: []string{"s1"},
			Filter:  &Filter{Column: "event_name", Values: []string{"Created"}},
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

func TestLoadEngineDefaults(t *testing.T) {
	e, err := LoadEngine(writeEngineConfig(t, baseEngineConfig))
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
	if _, err := LoadEngine(writeEngineConfig(t, production)); err == nil {
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
	if _, err := LoadEngine(writeEngineConfig(t, withThreeConfig)); err != nil {
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
	_, err := LoadEngine(writeEngineConfig(t, withoutControl))
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
	_, err := LoadEngine(writeEngineConfig(t, cfg))
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
	_, err := LoadEngine(writeEngineConfig(t, cfg))
	if err == nil {
		t.Fatal("expected a reaction destination with replay_endpoint to fail")
	}
	if !strings.Contains(err.Error(), "reaction destinations must not set replay_endpoint") {
		t.Fatalf("expected error to mention reaction replay_endpoint, got %q", err.Error())
	}
}

func TestPolicyForDefaultsToReaction(t *testing.T) {
	e, err := LoadEngine(writeEngineConfig(t, baseEngineConfig+`destinations:
  Accounts_Projection:
    kind: projection
    replay_endpoint: http://receiver:8080/rebuild/projections/accounts
`))
	if err != nil {
		t.Fatalf("expected config to load, got error: %v", err)
	}

	if got := e.PolicyFor("Accounts_Projection"); got != (DestinationPolicy{Kind: "projection", ReplayEndpoint: "http://receiver:8080/rebuild/projections/accounts"}) {
		t.Fatalf("expected listed destination's policy, got %+v", got)
	}
	if got := e.PolicyFor("Unlisted_Reaction"); got != (DestinationPolicy{Kind: "reaction"}) {
		t.Fatalf("expected unlisted destination to default to reaction, got %+v", got)
	}
}

func TestExampleEngineConfigLoads(t *testing.T) {
	t.Setenv("POSTIE_DATABASE_URL", "postgres://control-store")

	// application_config in examples/postie.yaml is a path relative to the
	// repo root; LoadEngine never reads it, so only LoadEngine is exercised.
	path := filepath.Join("..", "..", "examples", "postie.yaml")
	e, err := LoadEngine(path)
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
	want := DestinationPolicy{Kind: "projection", ReplayEndpoint: "http://receiver:8080/rebuild/projections/accounts"}
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
	if _, err := LoadEngine(writeEngineConfig(t, cfg)); err == nil {
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
	_, err := LoadEngine(writeEngineConfig(t, cfg))
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
	e, err := LoadEngine(writeEngineConfig(t, cfg))
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

func TestLoadReadsEngineThenApplication(t *testing.T) {
	t.Setenv("SOURCE_PASSWORD", "src-pass")
	t.Setenv("DEST_PASSWORD", "dest-pass")

	appPath := writeEngineConfig(t, validApplication)
	engine, application, err := Load(writeEngineConfig(t, baseEngineConfig+"application_config: "+appPath+"\n"))
	if err != nil {
		t.Fatalf("expected config to load, got error: %v", err)
	}
	if engine.Kafka.Partitions != 10 {
		t.Fatalf("expected default partitions 10, got %d", engine.Kafka.Partitions)
	}
	if len(application.Sources) != 2 {
		t.Fatalf("expected 2 sources, got %d", len(application.Sources))
	}
	if len(application.Destinations) != 2 {
		t.Fatalf("expected 2 destinations, got %d", len(application.Destinations))
	}
}

func TestLoadStopsAtEngineError(t *testing.T) {
	engine, application, err := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err == nil {
		t.Fatal("expected a missing engine config to fail")
	}
	if !reflect.DeepEqual(engine, Engine{}) {
		t.Fatalf("expected a zero-valued Engine, got %+v", engine)
	}
	if !reflect.DeepEqual(application, Application{}) {
		t.Fatalf("expected a zero-valued Application, got %+v", application)
	}
}

// Mutation testing showed no test sat on either side of these two limits.

func TestLoadEnginePartitionBoundary(t *testing.T) {
	withPartitions := func(n string) string {
		return strings.Replace(baseEngineConfig, "brokers: [kafka:9092]", "brokers: [kafka:9092]\n  partitions: "+n, 1)
	}
	e, err := LoadEngine(writeEngineConfig(t, withPartitions("1")))
	if err != nil || e.Kafka.Partitions != 1 {
		t.Fatalf("expected a single partition to be accepted, got %d, %v", e.Kafka.Partitions, err)
	}
	if _, err := LoadEngine(writeEngineConfig(t, withPartitions("-1"))); err == nil {
		t.Fatal("expected negative partitions to be rejected")
	}
}

func TestSourcePortBoundary(t *testing.T) {
	app := func(port int) Application {
		return Application{Sources: []Source{{ID: "s", Description: "s", Type: SourcePostgres, Host: "h", Port: port, Username: "u", Database: "d", Table: "t", Columns: []string{"id", "k"}, SerialColumn: "id", PartitioningColumn: "k"}}}
	}
	if err := app(1).Validate(); err != nil {
		t.Fatalf("expected port 1 to be accepted, got %v", err)
	}
	if err := app(0).Validate(); err == nil || !strings.Contains(err.Error(), "incomplete PostgreSQL connection") {
		t.Fatalf("expected port 0 to be rejected as incomplete, got %v", err)
	}
}

func TestOperatorToken(t *testing.T) {
	notCalled := func(string) ([]byte, error) {
		t.Fatal("readFile should not be called when file is empty")
		return nil, nil
	}

	t.Run("file wins over env", func(t *testing.T) {
		readFile := func(path string) ([]byte, error) {
			if path != "token.txt" {
				t.Fatalf("unexpected path %q", path)
			}
			return []byte("file-token"), nil
		}
		token, err := OperatorToken("env-token", "token.txt", readFile)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if token != "file-token" {
			t.Fatalf("got %q, want %q", token, "file-token")
		}
	})

	t.Run("trailing newline trimmed", func(t *testing.T) {
		readFile := func(string) ([]byte, error) {
			return []byte("file-token\n"), nil
		}
		token, err := OperatorToken("", "token.txt", readFile)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if token != "file-token" {
			t.Fatalf("got %q, want %q", token, "file-token")
		}
	})

	t.Run("unreadable file is an error, not a fallback", func(t *testing.T) {
		readFile := func(string) ([]byte, error) {
			return nil, errors.New("permission denied")
		}
		token, err := OperatorToken("env-token", "token.txt", readFile)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if token != "" {
			t.Fatalf("expected empty token on error, got %q", token)
		}
	})

	t.Run("empty env with no file is an error", func(t *testing.T) {
		token, err := OperatorToken("", "", notCalled)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if token != "" {
			t.Fatalf("expected empty token on error, got %q", token)
		}
	})

	t.Run("whitespace-only file is an error", func(t *testing.T) {
		readFile := func(string) ([]byte, error) {
			return []byte("   \n\t"), nil
		}
		token, err := OperatorToken("env-token", "token.txt", readFile)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if token != "" {
			t.Fatalf("expected empty token on error, got %q", token)
		}
	})
}

func TestRegression_EnvironmentStringsPreserved(t *testing.T) {
	values := []string{"alpha #beta", "alpha: beta", `embedded "double" quote`, `literal\backslash`, "actual\nnewline", "null", "true", "001", "${OTHER}"}
	styles := map[string]func(string) string{
		"unquoted":      func(value string) string { return "${SECRET}" },
		"single-quoted": func(value string) string { return "'${SECRET}'" },
		"double-quoted": func(value string) string { return `"${SECRET}"` },
	}
	for _, value := range values {
		for style, scalar := range styles {
			t.Run(style+"/"+value, func(t *testing.T) {
				t.Setenv("SECRET", value)
				t.Setenv("OTHER", "must-not-expand")
				app := `
data_sources:
  - id: source
    description: source
    type: postgres
    host: db
    port: 5432
    username: engine
    password: ` + scalar(value) + `
    database: app
    table: events
    columns: [id, correlation_id]
    serialColumn: id
    partitioningColumn: correlation_id
data_destinations: []
`
				loadedApp, err := LoadApplication(writeEngineConfig(t, app))
				if err != nil {
					t.Fatalf("LoadApplication() error = %v", err)
				}
				if got := loadedApp.Sources[0].Password; got != value {
					t.Errorf("LoadApplication() password = %q, want %q", got, value)
				}

				engine := strings.Replace(baseEngineConfig, "namespace: ns", "namespace: "+scalar(value), 1)
				loadedEngine, err := LoadEngine(writeEngineConfig(t, engine))
				if err != nil {
					t.Fatalf("LoadEngine() error = %v", err)
				}
				if got := loadedEngine.Namespace; got != value {
					t.Errorf("LoadEngine() namespace = %q, want %q", got, value)
				}
			})
		}
	}
}

func TestRegression_EnvironmentStructurePreserved(t *testing.T) {
	for _, name := range []string{"SECRET", "REG_COMMENT_MUST_BE_IGNORED", "REG_KEY_MUST_BE_IGNORED", "OTHER"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
	}
	t.Setenv("SECRET", "anchored")
	app := `
# ${REG_COMMENT_MUST_BE_IGNORED}
"${REG_KEY_MUST_BE_IGNORED}": ignored
data_sources:
  - id: source
    description: source
    type: postgres
    host: db
    port: 5432
    username: engine
    database: app
    table: events
    columns: [id, correlation_id]
    serialColumn: id
    partitioningColumn: correlation_id
data_destinations:
  - id: destination
    description: destination
    type: http-push
    endpoint: http://example.test
    username: engine
    sources: [source]
    filter:
      column: value
      values:
        - &regression_anchor ${SECRET}
        - *regression_anchor
`
	loaded, err := LoadApplication(writeEngineConfig(t, app))
	if err != nil {
		t.Fatalf("LoadApplication() structural fixture error = %v", err)
	}
	if got, want := loaded.Destinations[0].Filter.Values, []string{"anchored", "anchored"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("anchor and alias values = %#v, want %#v", got, want)
	}
	engine := `
# ${REG_COMMENT_MUST_BE_IGNORED}
"${REG_KEY_MUST_BE_IGNORED}": ignored` + strings.Replace(baseEngineConfig, "namespace: ns", "namespace: ${SECRET}", 1)
	loadedEngine, err := LoadEngine(writeEngineConfig(t, engine))
	if err != nil {
		t.Fatalf("LoadEngine() structural fixture error = %v", err)
	}
	if loadedEngine.Namespace != "anchored" {
		t.Fatalf("LoadEngine() namespace = %q, want %q", loadedEngine.Namespace, "anchored")
	}
}

func TestRegression_EnvironmentTypedFields(t *testing.T) {
	t.Setenv("DURATION", "1500ms")
	t.Setenv("REG_PORT", "5432")
	engine := strings.Replace(baseEngineConfig, "brokers: [kafka:9092]", "brokers: [kafka:9092]\n  partitions: 001\n  replication_factor: 001", 1) + `delivery:
  request_timeout: ${DURATION}
  drain_timeout: 2s
`
	loaded, err := LoadEngine(writeEngineConfig(t, engine))
	if err != nil {
		t.Fatalf("LoadEngine() typed fixture error = %v", err)
	}
	if loaded.Kafka.Partitions != 1 || loaded.Kafka.ReplicationFactor != 1 {
		t.Errorf("literal numeric config fields = partitions %d, replication factor %d; want 1, 1", loaded.Kafka.Partitions, loaded.Kafka.ReplicationFactor)
	}
	if loaded.Delivery.RequestTimeout != 1500*time.Millisecond {
		t.Errorf("request timeout = %s, want 1.5s", loaded.Delivery.RequestTimeout)
	}

	app := `
data_sources:
  - id: source
    description: source
    type: postgres
    host: db
    port: ${REG_PORT}
    username: engine
    database: app
    table: events
    columns: [id, correlation_id]
    serialColumn: id
    partitioningColumn: correlation_id
data_destinations: []
`
	if _, err := LoadApplication(writeEngineConfig(t, app)); err == nil {
		t.Fatal("LoadApplication() accepted an environment placeholder for an integer field")
	}
}

func TestRegression_EnvironmentMissingNamesAggregateAcrossScalars(t *testing.T) {
	for _, name := range []string{"REG_MISSING_A", "REG_MISSING_Z"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
	}
	content := `
data_sources:
  - id: source
    description: source
    type: postgres
    host: db
    port: 5432
    username: ${REG_MISSING_Z}
    password: ${REG_MISSING_A}
    database: ${REG_MISSING_Z}
    table: events
    columns: [id, correlation_id]
    serialColumn: id
    partitioningColumn: correlation_id
data_destinations: []
`
	_, err := LoadApplication(writeEngineConfig(t, content))
	if err == nil {
		t.Fatal("LoadApplication() accepted missing environment variables")
	}
	want := "environment variables REG_MISSING_A, REG_MISSING_Z are not set"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("LoadApplication() error = %q, want sorted and deduplicated names %q", err, want)
	}
}

func TestLoadApplicationNamesTheOneMissingVariable(t *testing.T) {
	t.Setenv("DEST_PASSWORD", "dest-pass")
	t.Setenv("SOURCE_PASSWORD", "")
	os.Unsetenv("SOURCE_PASSWORD")
	_, err := LoadApplication(writeEngineConfig(t, validApplication))
	if err == nil || !strings.Contains(err.Error(), "environment variable SOURCE_PASSWORD is not set") {
		t.Fatalf("got %v", err)
	}
}
