package debezium

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/rafaeelricco/postie/internal/provision"
	"github.com/rafaeelricco/postie/internal/stream"
)

// decoded Connect responses: no HTTP server involved.
func TestStatusFrom(t *testing.T) {
	cases := []struct {
		name string
		json string
		want provision.Status
	}{
		{
			name: "connector-failed",
			json: `{"connector":{"state":"FAILED","trace":"boom"},"tasks":[]}`,
			want: provision.Status{Connector: provision.StateFailed, Failed: true, Trace: "boom"},
		},
		{
			name: "task-failed-with-trace",
			json: `{"connector":{"state":"RUNNING"},"tasks":[{"state":"RUNNING"},{"state":"FAILED","trace":"task exploded"}]}`,
			want: provision.Status{Connector: provision.StateRunning, Tasks: []provision.ConnectorState{provision.StateRunning, provision.StateFailed}, Failed: true, Trace: "task exploded"},
		},
		{
			name: "all-running",
			json: `{"connector":{"state":"RUNNING"},"tasks":[{"state":"RUNNING"},{"state":"RUNNING"}]}`,
			want: provision.Status{Connector: provision.StateRunning, Tasks: []provision.ConnectorState{provision.StateRunning, provision.StateRunning}},
		},
		{
			name: "no-tasks",
			json: `{"connector":{"state":"RUNNING"},"tasks":[]}`,
			want: provision.Status{Connector: provision.StateRunning},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var raw connectStatus
			if err := json.Unmarshal([]byte(c.json), &raw); err != nil {
				t.Fatalf("json.Unmarshal: %v", err)
			}
			got := statusFrom(raw)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("statusFrom = %+v, want %+v", got, c.want)
			}
		})
	}
}

func TestClientAppliesConfigurationAndReadsRunningStatus(t *testing.T) {
	for _, code := range []int{http.StatusOK, http.StatusCreated} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			requests := make(chan string, 2)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- r.Method + " " + r.URL.Path
				if r.Method == http.MethodPut {
					var cfg map[string]string
					if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
						t.Error(err)
					}
					if r.Header.Get("Content-Type") != "application/json" || cfg["database.hostname"] != "db.internal" || cfg["database.password"] != testConnection().Password || cfg["slot.name"] != "existing_slot" || cfg["publication.autocreate.mode"] != "disabled" {
						t.Error("connector request lost its configured transport fields")
					}
					w.WriteHeader(code)
					return
				}
				fmt.Fprint(w, `{"connector":{"state":"RUNNING"},"tasks":[{"state":"RUNNING"}]}`)
			}))
			defer server.Close()
			source := testSource()
			// A nil HTTP client uses the default transport for both operations.
			client := Client{URL: server.URL + "/", Connections: map[string]Connection{source.ID: testConnection()}}
			names := stream.Names{Connector: "existing_connector", Slot: "existing_slot"}
			if err := client.EnsureConnector(context.Background(), source, testIdentity(source), names, provision.PublicationExternal); err != nil {
				t.Fatal(err)
			}
			status, err := client.ConnectorStatus(context.Background(), names.Connector)
			if err != nil || status.Failed || status.Connector != provision.StateRunning || len(status.Tasks) != 1 || status.Tasks[0] != provision.StateRunning {
				t.Fatalf("status = %+v, %v", status, err)
			}
			for _, want := range []string{"PUT /connectors/existing_connector/config", "GET /connectors/existing_connector/status"} {
				if got := <-requests; got != want {
					t.Fatalf("request=%q, want %q", got, want)
				}
			}
		})
	}
}

func TestClientRejectsMalformedConnectorEndpoint(t *testing.T) {
	client := Client{URL: "http://invalid\nendpoint", Connections: map[string]Connection{}}
	source := testSource()
	err := client.EnsureConnector(context.Background(), source, testIdentity(source), stream.Names{Connector: "existing"}, provision.PublicationManaged)
	if err == nil || !strings.Contains(err.Error(), "capture: build connector request") {
		t.Fatalf("malformed endpoint error = %v", err)
	}
}

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

func testSourceAndIdentity() (stream.Source, stream.Identity) {
	source := stream.Source{
		ID:                 "type-matrix",
		Description:        "type matrix",
		Table:              "type_matrix",
		Columns:            []string{"id", "partition_key", "c_int8", "c_float8", "c_bool", "c_json", "c_bytea", "c_timestamp", "c_timestamptz", "c_text"},
		SerialColumn:       "id",
		PartitioningColumn: "partition_key",
	}
	identity := stream.Identity{
		Table:              source.Table,
		SerialColumn:       source.SerialColumn,
		PartitioningColumn: source.PartitioningColumn,
		Columns: []stream.Column{
			{Name: "id", Type: stream.PGInt8},
			{Name: "partition_key", Type: stream.PGText},
			{Name: "c_int8", Type: stream.PGInt8},
			{Name: "c_float8", Type: stream.PGFloat8},
			{Name: "c_bool", Type: stream.PGBool},
			{Name: "c_json", Type: stream.PGJSON},
			{Name: "c_bytea", Type: stream.PGBytea},
			{Name: "c_timestamp", Type: stream.PGTimestamp},
			{Name: "c_timestamptz", Type: stream.PGTimestamptz},
			{Name: "c_text", Type: stream.PGText},
		},
		Partitions: 3,
	}
	return source, identity
}

func testRecord(value, key string) *stream.RawRecord {
	return &stream.RawRecord{Topic: "postie.production.type-matrix.g1.public.type_matrix", Partition: 2, Offset: 17, Value: []byte(value), Key: []byte(key)}
}

func TestDecodeRealDebeziumShape(t *testing.T) {
	source, identity := testSourceAndIdentity()
	record := testRecord(`{"before":null,"after":{"id":9223372036854775807,"partition_key":"row-ordinary","c_int8":-9223372036854775808,"c_float8":1e300,"c_bool":true,"c_json":"{\"b\":2,\"a\":1}","c_bytea":"3q2+7w==","c_timestamp":1704164645123456,"c_timestamptz":"2024-01-02T03:04:05.123456+05:30","c_text":"hello"},"source":{"schema":"public","table":"type_matrix"},"op":"c"}`, `{"partition_key":"row-ordinary"}`)

	got, err := Decode(source, identity, 1, record)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if !reflect.DeepEqual(got.Source, source) || got.Topic != record.Topic || got.Partition != record.Partition || got.Offset != record.Offset || got.Generation != 1 {
		t.Fatalf("record metadata mismatch: %+v", got)
	}
	if got.EventID != "" {
		t.Fatalf("EventID = %q, want empty", got.EventID)
	}
	want := `{"c_bool":true,"c_bytea":"3q2+7w==","c_float8":` + "1" + strings.Repeat("0", 300) + `,"c_int8":-9223372036854775808,"c_json":"{\"b\":2,\"a\":1}","c_text":"hello","c_timestamp":"2024-01-02 03:04:05.123456","c_timestamptz":"2024-01-01 21:34:05.123456+00","id":9223372036854775807,"partition_key":"row-ordinary"}`
	if string(got.Payload) != want {
		t.Fatalf("Payload = %s, want %s", got.Payload, want)
	}
}

func TestDecodeEventIDAndNulls(t *testing.T) {
	source, identity := testSourceAndIdentity()
	source.Columns = append(source.Columns, "event_id")
	identity.Columns = append(identity.Columns, stream.Column{Name: "event_id", Type: stream.PGText})
	identity.EventIDColumn = "event_id"
	record := testRecord(`{"after":{"id":1,"partition_key":"p","c_int8":null,"c_float8":null,"c_bool":null,"c_json":"{}","c_bytea":"","c_timestamp":null,"c_timestamptz":null,"c_text":null,"event_id":"evt-1"},"source":{"schema":"public","table":"type_matrix"},"op":"r"}`, `{"partition_key":"p"}`)

	got, err := Decode(source, identity, 2, record)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got.EventID != "evt-1" {
		t.Fatalf("EventID = %q, want evt-1", got.EventID)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(got.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if _, ok := payload["event_id"]; !ok {
		t.Fatal("event_id missing from payload")
	}
}

func TestDecodeRequiresConfiguredEventID(t *testing.T) {
	source, identity := testSourceAndIdentity()
	source.Columns = append(source.Columns, "event_id")
	identity.Columns = append(identity.Columns, stream.Column{Name: "event_id", Type: stream.PGText})
	identity.EventIDColumn = "event_id"
	base := `{"after":{"id":1,"partition_key":"p","c_int8":null,"c_float8":null,"c_bool":null,"c_json":"{}","c_bytea":"","c_timestamp":null,"c_timestamptz":null,"c_text":null,"event_id":%s},"source":{"schema":"public","table":"type_matrix"},"op":"c"}`
	for _, value := range []string{"null", `""`, `1`} {
		t.Run(value, func(t *testing.T) {
			_, err := Decode(source, identity, 1, testRecord(fmt.Sprintf(base, value), `{"partition_key":"p"}`))
			if err == nil {
				t.Fatal("Decode() error = nil, want configured event ID rejection")
			}
		})
	}
}

func TestFloatDecimalExpansion(t *testing.T) {
	source := stream.Source{Table: "numbers", Columns: []string{"id", "key", "small", "large"}, SerialColumn: "id", PartitioningColumn: "key"}
	identity := stream.Identity{
		Table: source.Table, SerialColumn: source.SerialColumn, PartitioningColumn: source.PartitioningColumn, Partitions: 1,
		Columns: []stream.Column{{Name: "id", Type: stream.PGInt8}, {Name: "key", Type: stream.PGText}, {Name: "small", Type: stream.PGFloat4}, {Name: "large", Type: stream.PGFloat8}},
	}
	cases := []struct {
		name, small, large, wantSmall, wantLarge string
	}{
		{"large exponent", "1.2e2", "1e300", "120", "1" + strings.Repeat("0", 300)},
		{"negative fractions", "-1.25e-2", "-1.2300e-2", "-0.0125", "-0.012300"},
		{"ordinary", "3.14", "0.1", "3.14", "0.1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			value := fmt.Sprintf(`{"after":{"id":1,"key":"k","small":%s,"large":%s},"source":{"schema":"public","table":"numbers"},"op":"c"}`, tc.small, tc.large)
			got, err := Decode(source, identity, 1, &stream.RawRecord{Topic: "numbers", Key: []byte(`{"key":"k"}`), Value: []byte(value)})
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			var payload map[string]json.RawMessage
			if err := json.Unmarshal(got.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if string(payload["small"]) != tc.wantSmall || string(payload["large"]) != tc.wantLarge {
				t.Fatalf("float payload = small %s large %s, want small %s large %s", payload["small"], payload["large"], tc.wantSmall, tc.wantLarge)
			}
		})
	}
}

func TestDecodeRejectsInvalidConfiguredValues(t *testing.T) {
	source, identity := testSourceAndIdentity()
	base := `{"after":{"id":1,"partition_key":"p","c_int8":null,"c_float8":null,"c_bool":null,"c_json":"{}","c_bytea":"","c_timestamp":null,"c_timestamptz":null,"c_text":null},"source":{"schema":"public","table":"type_matrix"},"op":"c"}`
	cases := []struct{ field, value string }{
		{"c_int8", `"bad"`}, {"c_float8", `"bad"`}, {"c_bool", `1`}, {"c_json", `"bad"`},
		{"c_bytea", `"!"`}, {"c_text", `1`}, {"c_timestamp", `"bad"`}, {"c_timestamptz", `"bad"`},
	}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			var envelope map[string]json.RawMessage
			if err := json.Unmarshal([]byte(base), &envelope); err != nil {
				t.Fatal(err)
			}
			var after map[string]json.RawMessage
			if err := json.Unmarshal(envelope["after"], &after); err != nil {
				t.Fatal(err)
			}
			after[tc.field] = json.RawMessage(tc.value)
			envelope["after"], _ = json.Marshal(after)
			raw, err := json.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			value := string(raw)
			if _, err := Decode(source, identity, 1, testRecord(value, `{"partition_key":"p"}`)); err == nil {
				t.Fatal("Decode() error = nil, want malformed value error")
			}
		})
	}
}

func TestDecodeRejectsMalformedEnvelopeAndKeyShapes(t *testing.T) {
	source, identity := testSourceAndIdentity()
	valid := `{"after":{"id":1,"partition_key":"p","c_int8":null,"c_float8":null,"c_bool":null,"c_json":"{}","c_bytea":"","c_timestamp":null,"c_timestamptz":null,"c_text":null},"source":{"schema":"public","table":"type_matrix"},"op":"c"}`
	cases := []struct{ name, value, key string }{
		{"nil record", valid, `{"partition_key":"p"}`},
		{"missing op", `{"after":{}}`, `{"partition_key":"p"}`},
		{"source scalar", `{"op":"c","source":1,"after":{}}`, `{"partition_key":"p"}`},
		{"source missing schema", `{"op":"c","source":{"table":"type_matrix"},"after":{}}`, `{"partition_key":"p"}`},
		{"wrong schema", strings.Replace(valid, `"public"`, `"private"`, 1), `{"partition_key":"p"}`},
		{"after array", `{"op":"c","source":{"schema":"public","table":"type_matrix"},"after":[]}`, `{"partition_key":"p"}`},
		{"key nil", valid, ""},
		{"key scalar", valid, `1`},
		{"key extra", valid, `{"partition_key":"p","other":1}`},
		{"key missing", valid, `{"other":"p"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var record *stream.RawRecord
			if tc.name == "nil record" {
				record = nil
			} else {
				record = testRecord(tc.value, tc.key)
			}
			if _, err := Decode(source, identity, 1, record); err == nil {
				t.Fatal("Decode() error = nil, want malformed shape error")
			}
		})
	}
}

func TestDecodeRejectsUnsupportedOpsAndMalformedRecords(t *testing.T) {
	source, identity := testSourceAndIdentity()
	cases := []struct {
		name  string
		value string
		key   string
	}{
		{"update", `{"after":{},"source":{"schema":"public","table":"type_matrix"},"op":"u"}`, `{"partition_key":"p"}`},
		{"delete", `{"after":null,"source":{"schema":"public","table":"type_matrix"},"op":"d"}`, `{"partition_key":"p"}`},
		{"truncate", `{"after":{},"source":{"schema":"public","table":"type_matrix"},"op":"t"}`, `{"partition_key":"p"}`},
		{"malformed", `{"after":`, `{"partition_key":"p"}`},
		{"wrong-table", `{"after":{},"source":{"schema":"public","table":"other"},"op":"c"}`, `{"partition_key":"p"}`},
		{"wrong-key", `{"after":{"id":1,"partition_key":"p","c_int8":null,"c_float8":null,"c_bool":null,"c_json":"{}","c_bytea":"","c_timestamp":null,"c_timestamptz":null,"c_text":null},"source":{"schema":"public","table":"type_matrix"},"op":"c"}`, `{"partition_key":"other"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decode(source, identity, 1, testRecord(tc.value, tc.key))
			if err == nil {
				t.Fatal("Decode() error = nil, want error")
			}
			if strings.Contains(err.Error(), tc.value) || strings.Contains(err.Error(), tc.key) {
				t.Fatalf("error contains payload content: %v", err)
			}
		})
	}
}

func TestDecodeRejectsInvalidIdentityAndValues(t *testing.T) {
	source, identity := testSourceAndIdentity()
	cases := []struct {
		name string
		mut  func(*stream.Identity, *stream.Source)
	}{
		{"zero generation", func(_ *stream.Identity, _ *stream.Source) {}},
		{"zero partitions", func(id *stream.Identity, _ *stream.Source) { id.Partitions = 0 }},
		{"missing column", func(id *stream.Identity, _ *stream.Source) { id.Columns = id.Columns[:len(id.Columns)-1] }},
		{"null serial", func(_ *stream.Identity, _ *stream.Source) {}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, src := identity, source
			generation := stream.Generation(1)
			if tc.name == "zero generation" {
				generation = 0
			}
			tc.mut(&id, &src)
			value := `{"after":{"id":1,"partition_key":"p","c_int8":null,"c_float8":null,"c_bool":null,"c_json":"{}","c_bytea":"","c_timestamp":null,"c_timestamptz":null,"c_text":null},"source":{"schema":"public","table":"type_matrix"},"op":"c"}`
			if tc.name == "null serial" {
				value = `{"after":{"id":null,"partition_key":"p","c_int8":null,"c_float8":null,"c_bool":null,"c_json":"{}","c_bytea":"","c_timestamp":null,"c_timestamptz":null,"c_text":null},"source":{"schema":"public","table":"type_matrix"},"op":"c"}`
			}
			if _, err := Decode(src, id, generation, testRecord(value, `{"partition_key":"p"}`)); err == nil {
				t.Fatal("Decode() error = nil, want error")
			}
		})
	}
}

func FuzzDecode(f *testing.F) {
	source, identity := testSourceAndIdentity()
	f.Add([]byte(`{"after":{},"source":{"schema":"public","table":"type_matrix"},"op":"c"}`), []byte(`{"partition_key":"p"}`))
	f.Fuzz(func(t *testing.T, value, key []byte) {
		_, _ = Decode(source, identity, 1, &stream.RawRecord{Topic: "topic", Value: value, Key: key})
	})
}

func TestRegression_SnapshotTableIsQuoted(t *testing.T) {
	src := stream.Source{Table: "Event_Store", SerialColumn: "id", Columns: []string{"id"}}
	id := stream.Identity{Columns: []stream.Column{{Name: "id", Type: stream.PGInt8}}}
	n := provision.NamesFor("ns", "dev", src, 1)
	cfg := ConnectorConfig(src, Connection{}, id, n, provision.PublicationManaged)
	if got, want := cfg["snapshot.select.statement.overrides.public.Event_Store"], `SELECT "id" FROM public."Event_Store" ORDER BY "id" ASC`; got != want {
		t.Fatalf("snapshot override = %q, want %q", got, want)
	}
}
