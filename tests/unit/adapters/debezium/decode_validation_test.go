package debezium_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/rafaeelricco/postie/internal/adapters/debezium"
	"github.com/rafaeelricco/postie/internal/stream"
)

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

	got, err := debezium.Decode(source, identity, 1, record)
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

	got, err := debezium.Decode(source, identity, 2, record)
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
			_, err := debezium.Decode(source, identity, 1, testRecord(fmt.Sprintf(base, value), `{"partition_key":"p"}`))
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
			got, err := debezium.Decode(source, identity, 1, &stream.RawRecord{Topic: "numbers", Key: []byte(`{"key":"k"}`), Value: []byte(value)})
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
			if _, err := debezium.Decode(source, identity, 1, testRecord(value, `{"partition_key":"p"}`)); err == nil {
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
			if _, err := debezium.Decode(source, identity, 1, record); err == nil {
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
			_, err := debezium.Decode(source, identity, 1, testRecord(tc.value, tc.key))
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
			if _, err := debezium.Decode(src, id, generation, testRecord(value, `{"partition_key":"p"}`)); err == nil {
				t.Fatal("Decode() error = nil, want error")
			}
		})
	}
}

func FuzzDecode(f *testing.F) {
	source, identity := testSourceAndIdentity()
	f.Add([]byte(`{"after":{},"source":{"schema":"public","table":"type_matrix"},"op":"c"}`), []byte(`{"partition_key":"p"}`))
	f.Fuzz(func(t *testing.T, value, key []byte) {
		_, _ = debezium.Decode(source, identity, 1, &stream.RawRecord{Topic: "topic", Value: value, Key: key})
	})
}
