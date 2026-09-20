package debezium

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/rafaeelricco/postie/internal/provision"
	"github.com/rafaeelricco/postie/internal/stream"
)

func testSource() stream.Source {
	return stream.Source{ID: "orders", Description: "orders db", Table: "event_store", Columns: []string{"id", "correlation_id", "payload"}, SerialColumn: "id", PartitioningColumn: "correlation_id"}
}

func testConnection() Connection {
	return Connection{Host: "db.internal", Port: 5432, Username: "engine", Password: "s3cr3t-password", Database: "app"}
}

func testIdentity(src stream.Source) stream.Identity {
	columns := make([]stream.Column, len(src.Columns))
	for i, name := range src.Columns {
		columns[i] = stream.Column{Name: name, Type: stream.PGText}
	}
	return stream.Identity{Table: src.Table, SerialColumn: src.SerialColumn, PartitioningColumn: src.PartitioningColumn, Columns: columns}
}

func TestConnectorConfigKeyIsPartitioningColumn(t *testing.T) {
	src := testSource()
	n := provision.NamesFor("postie", "production", src, 1)
	cfg := ConnectorConfig(src, testConnection(), testIdentity(src), n, provision.PublicationManaged)

	if got, want := cfg["message.key.columns"], "public.event_store:correlation_id"; got != want {
		t.Errorf("message.key.columns = %q, want %q", got, want)
	}
}

func TestConnectorConfigCoreSettings(t *testing.T) {
	src := testSource()
	n := provision.NamesFor("postie", "production", src, 1)
	cfg := ConnectorConfig(src, testConnection(), testIdentity(src), n, provision.PublicationManaged)

	want := map[string]string{
		"connector.class":                           "io.debezium.connector.postgresql.PostgresConnector",
		"plugin.name":                               "pgoutput",
		"snapshot.mode":                             "initial",
		"skipped.operations":                        "none",
		"tombstones.on.delete":                      "false",
		"binary.handling.mode":                      "base64",
		"decimal.handling.mode":                     "string",
		"time.precision.mode":                       "adaptive_time_microseconds",
		"heartbeat.interval.ms":                     "10000",
		"topic.creation.default.replication.factor": "-1",
		"topic.creation.default.partitions":         "1",
		"key.converter":                             "org.apache.kafka.connect.json.JsonConverter",
		"key.converter.schemas.enable":              "false",
		"value.converter":                           "org.apache.kafka.connect.json.JsonConverter",
		"value.converter.schemas.enable":            "false",
		"producer.override.acks":                    "all",
	}
	for key, value := range want {
		if got := cfg[key]; got != value {
			t.Errorf("cfg[%q] = %q, want %q", key, got, value)
		}
	}

	// Fields derived from input still land under the expected keys.
	for _, key := range []string{
		"database.hostname", "database.port", "database.user", "database.password", "database.dbname",
		"topic.prefix", "table.include.list", "column.include.list", "message.key.columns",
		"slot.name", "publication.name",
		"snapshot.select.statement.overrides", "snapshot.select.statement.overrides.public.event_store",
	} {
		if _, ok := cfg[key]; !ok {
			t.Errorf("cfg missing key %q", key)
		}
	}

	if cfg["topic.prefix"] != n.TopicPrefix {
		t.Errorf("topic.prefix = %q, want %q", cfg["topic.prefix"], n.TopicPrefix)
	}
	if cfg["slot.name"] != n.Slot {
		t.Errorf("slot.name = %q, want %q", cfg["slot.name"], n.Slot)
	}
	if cfg["publication.name"] != n.Publication {
		t.Errorf("publication.name = %q, want %q", cfg["publication.name"], n.Publication)
	}
	if cfg["table.include.list"] != "public.event_store" {
		t.Errorf("table.include.list = %q, want %q", cfg["table.include.list"], "public.event_store")
	}
}

func TestConnectorConfigSnapshotOrdersBySerial(t *testing.T) {
	src := testSource()
	n := provision.NamesFor("postie", "production", src, 1)
	id := testIdentity(src)
	cfg := ConnectorConfig(src, testConnection(), id, n, provision.PublicationManaged)

	statement := cfg["snapshot.select.statement.overrides.public.event_store"]
	if !strings.HasSuffix(statement, `ORDER BY "id" ASC`) {
		t.Fatalf("snapshot override statement %q does not end with ORDER BY \"id\" ASC", statement)
	}
	if strings.Contains(statement, "*") {
		t.Errorf("snapshot override statement %q should not select *", statement)
	}
	for _, c := range id.Columns {
		if !strings.Contains(statement, `"`+c.Name+`"`) {
			t.Errorf("snapshot override statement %q missing configured column %q", statement, c.Name)
		}
	}
	if strings.Contains(statement, `"extra_column"`) {
		t.Errorf("snapshot override statement %q lists an unconfigured column", statement)
	}
}

func TestColumnIncludeListEscapes(t *testing.T) {
	src := testSource()
	src.Table = "event.store" // deliberately contains a regex metacharacter
	src.Columns = []string{"id", "amount+tax"}
	src.SerialColumn = "id"
	src.PartitioningColumn = "id"

	n := provision.NamesFor("postie", "production", src, 1)
	id := testIdentity(src)
	cfg := ConnectorConfig(src, testConnection(), id, n, provision.PublicationManaged)

	includeList := cfg["column.include.list"]
	for _, want := range []string{
		regexp.QuoteMeta("public.event.store.id"),
		regexp.QuoteMeta("public.event.store.amount+tax"),
	} {
		if !strings.Contains(includeList, want) {
			t.Errorf("column.include.list %q missing escaped entry %q", includeList, want)
		}
	}
	// Raw (unescaped) metacharacters must not survive.
	if strings.Contains(includeList, "event.store.amount+tax") && !strings.Contains(includeList, `event\.store\.amount\+tax`) {
		t.Errorf("column.include.list %q does not look escaped", includeList)
	}

	parts := strings.Split(includeList, ",")
	if len(parts) != len(src.Columns) {
		t.Errorf("column.include.list has %d entries, want %d", len(parts), len(src.Columns))
	}
}

func TestEnsureConnectorNeverLeaksPassword(t *testing.T) {
	const secret = "sup3r-s3cret-p4ssw0rd"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error, no idea what went wrong"))
	}))
	defer server.Close()

	src := testSource()
	connection := testConnection()
	connection.Password = secret
	n := provision.NamesFor("postie", "production", src, 1)
	cfg := ConnectorConfig(src, connection, testIdentity(src), n, provision.PublicationManaged)

	err := EnsureConnector(context.Background(), server.Client(), server.URL, n, cfg)
	if err == nil {
		t.Fatal("expected an error from a 500 response")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("error message leaks the database password: %v", err)
	}
	if strings.Contains(err.Error(), cfg["database.password"]) {
		t.Errorf("error message leaks database.password value: %v", err)
	}
}

func TestConnectorStatusMissing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	_, err := ConnectorStatus(context.Background(), server.Client(), server.URL, "missing-connector")
	if !errors.Is(err, provision.ErrConnectorMissing) {
		t.Fatalf("ConnectorStatus error = %v, want ErrConnectorMissing", err)
	}
}

// TestIdentityFromRejections is the Docker-free table test for every
// contract rule identityFrom enforces (the same rules InspectTable enforced
// before the pure/effectful split). It never touches Postgres: TableFacts is
// built by hand for each case.
// TestConnectorConfigPublicationMode asserts publication.autocreate.mode
// reflects the provision.PublicationMode passed in, independent of the other literal
// core settings covered by TestConnectorConfigCoreSettings.
func TestConnectorConfigPublicationMode(t *testing.T) {
	src := testSource()
	n := provision.NamesFor("postie", "production", src, 1)
	id := testIdentity(src)

	cases := []struct {
		mode provision.PublicationMode
		want string
	}{
		{provision.PublicationManaged, "filtered"},
		{provision.PublicationExternal, "disabled"},
	}
	for _, c := range cases {
		cfg := ConnectorConfig(src, testConnection(), id, n, c.mode)
		if got := cfg["publication.autocreate.mode"]; got != c.want {
			t.Errorf("mode %v: publication.autocreate.mode = %q, want %q", c.mode, got, c.want)
		}
	}
}
