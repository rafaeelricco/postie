package debezium_test

import (
	"testing"

	"github.com/rafaeelricco/postie/internal/adapters/debezium"
	"github.com/rafaeelricco/postie/internal/stream"
)

func TestDecodeCopiesRecordMetadataAndConvertsPayload(t *testing.T) {
	source := stream.Source{ID: "events", Description: "Events", Table: "event_store", Columns: []string{"id", "partition_key", "name"}, SerialColumn: "id", PartitioningColumn: "partition_key"}
	identity := stream.Identity{Table: source.Table, SerialColumn: source.SerialColumn, PartitioningColumn: source.PartitioningColumn, Partitions: 2, Columns: []stream.Column{{Name: "id", Type: stream.PGInt8}, {Name: "partition_key", Type: stream.PGText}, {Name: "name", Type: stream.PGText}}}
	raw := &stream.RawRecord{Topic: "events.topic", Partition: 2, Offset: 17, LeaderEpoch: 9, Key: []byte(`{"partition_key":"p"}`), Value: []byte(`{"after":{"id":1,"partition_key":"p","name":"created"},"source":{"schema":"public","table":"event_store"},"op":"c"}`)}
	record, err := debezium.Decode(source, identity, 3, raw)
	if err != nil {
		t.Fatal(err)
	}
	if record.Topic != raw.Topic || record.Partition != raw.Partition || record.Offset != raw.Offset || record.LeaderEpoch != raw.LeaderEpoch || record.Generation != 3 || record.Source.ID != source.ID {
		t.Fatalf("metadata mismatch: %+v", record)
	}
	if string(record.Payload) != `{"id":1,"name":"created","partition_key":"p"}` {
		t.Fatalf("payload=%s", record.Payload)
	}
}

func TestDecodeRejectsUnsupportedOperationAndMalformedKey(t *testing.T) {
	source := stream.Source{Table: "events", Columns: []string{"id", "key"}, SerialColumn: "id", PartitioningColumn: "key"}
	identity := stream.Identity{Table: source.Table, SerialColumn: source.SerialColumn, PartitioningColumn: source.PartitioningColumn, Partitions: 1, Columns: []stream.Column{{Name: "id", Type: stream.PGInt8}, {Name: "key", Type: stream.PGText}}}
	for _, value := range []string{`{"after":{},"source":{"schema":"public","table":"events"},"op":"u"}`, `{"after":{"id":1,"key":"p"},"source":{"schema":"public","table":"events"},"op":"c"}`} {
		_, err := debezium.Decode(source, identity, 1, &stream.RawRecord{Key: []byte(`{"key":"other"}`), Value: []byte(value)})
		if err == nil {
			t.Fatalf("accepted malformed record %s", value)
		}
	}
}
