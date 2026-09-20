// Package debezium decodes the schema-less records emitted by the capture
// connector. It depends only on the stream contract and Postie conversion.
package debezium

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/rafaeelricco/postie/internal/protocol"
	"github.com/rafaeelricco/postie/internal/stream"
)

func Decode(source stream.Source, identity stream.Identity, generation stream.Generation, record *stream.RawRecord) (stream.Record, error) {
	if record == nil {
		return stream.Record{}, errorsf("record is nil")
	}
	if err := validateIdentity(source, identity, generation); err != nil {
		return stream.Record{}, err
	}
	envelope, err := decodeObject(record.Value, "envelope")
	if err != nil {
		return stream.Record{}, err
	}
	op, err := stringField(envelope, "op", "envelope")
	if err != nil {
		return stream.Record{}, err
	}
	if op != "c" && op != "r" {
		return stream.Record{}, errorsf("unsupported operation")
	}
	if err := validateSource(envelope, source); err != nil {
		return stream.Record{}, err
	}
	afterRaw, ok := envelope["after"]
	if !ok || isNull(afterRaw) {
		return stream.Record{}, errorsf("operation has no after object")
	}
	after, err := decodeObject(afterRaw, "after")
	if err != nil {
		return stream.Record{}, err
	}
	if err := validateKey(record.Key, after, identity); err != nil {
		return stream.Record{}, err
	}

	values := make(map[string]json.RawMessage, len(identity.Columns))
	var eventID string
	for _, column := range identity.Columns {
		raw, ok := after[column.Name]
		if !ok {
			return stream.Record{}, errorsf("after object is missing configured column %q", column.Name)
		}
		if isNull(raw) && (column.Name == identity.SerialColumn || column.Name == identity.PartitioningColumn) {
			return stream.Record{}, errorsf("required column %q is null", column.Name)
		}
		converted, err := protocol.ConvertValue(column.Type, raw, column.Name)
		if err != nil {
			return stream.Record{}, errorsf("%v", err)
		}
		values[column.Name] = converted
		if column.Name == identity.EventIDColumn {
			if isNull(raw) {
				return stream.Record{}, errorsf("event id column %q is null", column.Name)
			}
			eventID, err = decodeString(raw, "event id "+column.Name)
			if err != nil {
				return stream.Record{}, err
			}
			if eventID == "" {
				return stream.Record{}, errorsf("event id column %q is empty", column.Name)
			}
		}
	}
	payload, err := json.Marshal(values)
	if err != nil {
		return stream.Record{}, errorsf("marshal configured payload")
	}
	return stream.Record{Source: source, Payload: payload, Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, LeaderEpoch: record.LeaderEpoch, EventID: eventID, Generation: generation}, nil
}

func validateIdentity(source stream.Source, identity stream.Identity, generation stream.Generation) error {
	if !generation.Valid() {
		return errorsf("generation is invalid")
	}
	if source.Table == "" || identity.Table == "" || identity.Table != source.Table {
		return errorsf("identity table does not match source table")
	}
	if identity.SerialColumn == "" || identity.PartitioningColumn == "" || identity.SerialColumn != source.SerialColumn || identity.PartitioningColumn != source.PartitioningColumn {
		return errorsf("identity columns do not match source configuration")
	}
	if identity.Partitions < 1 {
		return errorsf("identity has invalid partition count")
	}
	if len(source.Columns) == 0 || len(identity.Columns) != len(source.Columns) {
		return errorsf("identity columns do not match source configuration")
	}
	seen := make(map[string]bool, len(identity.Columns))
	serialFound, partitionFound, eventIDFound := false, false, identity.EventIDColumn == ""
	for i, column := range identity.Columns {
		if column.Name == "" || seen[column.Name] || source.Columns[i] != column.Name {
			return errorsf("identity has invalid configured columns")
		}
		seen[column.Name] = true
		if !supported(column.Type) {
			return errorsf("configured column %q has unsupported type", column.Name)
		}
		if column.Name == identity.SerialColumn {
			serialFound = true
			if !integerType(column.Type) {
				return errorsf("serial column %q has invalid type", column.Name)
			}
		}
		if column.Name == identity.PartitioningColumn {
			partitionFound = true
		}
		if column.Name == identity.EventIDColumn {
			eventIDFound = true
		}
	}
	if !serialFound || !partitionFound || !eventIDFound {
		return errorsf("identity is missing a required configured column")
	}
	return nil
}

func validateSource(envelope map[string]json.RawMessage, source stream.Source) error {
	raw, ok := envelope["source"]
	if !ok {
		return errorsf("envelope is missing source metadata")
	}
	metadata, err := decodeObject(raw, "source metadata")
	if err != nil {
		return err
	}
	schema, err := stringField(metadata, "schema", "source metadata")
	if err != nil {
		return err
	}
	table, err := stringField(metadata, "table", "source metadata")
	if err != nil {
		return err
	}
	if schema != "public" {
		return errorsf("source schema is not public")
	}
	if table != source.Table {
		return errorsf("source table does not match configured table")
	}
	return nil
}

func validateKey(raw []byte, after map[string]json.RawMessage, identity stream.Identity) error {
	key, err := decodeObject(raw, "Kafka key")
	if err != nil {
		return err
	}
	if len(key) != 1 {
		return errorsf("Kafka key must contain only the partition column")
	}
	keyValue, ok := key[identity.PartitioningColumn]
	if !ok || isNull(keyValue) {
		return errorsf("Kafka key is missing the partition column")
	}
	afterValue, ok := after[identity.PartitioningColumn]
	if !ok || isNull(afterValue) {
		return errorsf("after object has a null partition column")
	}
	for _, column := range identity.Columns {
		if column.Name == identity.PartitioningColumn {
			if _, err := protocol.ConvertValue(column.Type, keyValue, column.Name+" in Kafka key"); err != nil {
				return errorsf("%v", err)
			}
			break
		}
	}
	keyCompact, err := compact(keyValue)
	if err != nil {
		return errorsf("Kafka key has malformed partition value")
	}
	afterCompact, err := compact(afterValue)
	if err != nil || !bytes.Equal(keyCompact, afterCompact) {
		return errorsf("Kafka key does not match the partition column")
	}
	return nil
}

func decodeObject(raw []byte, name string) (map[string]json.RawMessage, error) {
	if len(raw) == 0 {
		return nil, errorsf("%s is empty", name)
	}
	var object map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&object); err != nil || object == nil {
		return nil, errorsf("%s is not a JSON object", name)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, errorsf("%s has trailing data", name)
	}
	return object, nil
}

func stringField(object map[string]json.RawMessage, name, context string) (string, error) {
	raw, ok := object[name]
	if !ok {
		return "", errorsf("%s is missing %q", context, name)
	}
	return decodeString(raw, context+"."+name)
}

func decodeString(raw []byte, field string) (string, error) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", errorsf("field %q is not a string", field)
	}
	return value, nil
}

func compact(raw []byte) ([]byte, error) {
	var out bytes.Buffer
	if err := json.Compact(&out, raw); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
func isNull(raw []byte) bool { return bytes.Equal(bytes.TrimSpace(raw), []byte("null")) }

func supported(typ stream.PGType) bool {
	switch typ {
	case stream.PGInt2, stream.PGInt4, stream.PGInt8, stream.PGFloat4, stream.PGFloat8, stream.PGBool, stream.PGJSON, stream.PGBytea, stream.PGTimestamp, stream.PGTimestamptz, stream.PGText:
		return true
	default:
		return false
	}
}
func integerType(typ stream.PGType) bool {
	return typ == stream.PGInt2 || typ == stream.PGInt4 || typ == stream.PGInt8
}
func errorsf(format string, args ...any) error { return fmt.Errorf("payload: "+format, args...) }
