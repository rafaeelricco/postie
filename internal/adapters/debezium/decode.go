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

// Decode turns one raw Debezium record into the stream.Record Postie delivers.
// It performs no I/O: the same inputs always give the same record or error.
//
// Only inserts ("c") and snapshot reads ("r") are accepted, the Kafka key must
// equal the partitioning column, and every configured column must be present.
// Errors never echo record values, which may hold customer data.
//
//	record, err := debezium.Decode(source, registered.Identity, generation, raw)
//	record.Payload // {"id":42,"tenant":"acme"}: the configured columns only
func Decode(source stream.Source, identity stream.Identity, generation stream.Generation, record *stream.RawRecord) (stream.Record, error) {
	if record == nil {
		return stream.Record{}, errorsf("record is nil")
	}
	if err := validateIdentity(source, identity, generation); err != nil {
		return stream.Record{}, err
	}
	after, err := afterObject(record, source, identity)
	if err != nil {
		return stream.Record{}, err
	}
	values, eventID, err := configuredValues(after, identity)
	if err != nil {
		return stream.Record{}, err
	}
	payload, err := json.Marshal(values)
	if err != nil {
		return stream.Record{}, errorsf("marshal configured payload")
	}
	return stream.Record{
		Source: source, Payload: payload, EventID: eventID, Generation: generation,
		Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, LeaderEpoch: record.LeaderEpoch,
	}, nil
}

// afterObject validates the envelope (operation, source metadata, Kafka key)
// and returns its "after" row image.
func afterObject(record *stream.RawRecord, source stream.Source, identity stream.Identity) (map[string]json.RawMessage, error) {
	envelope, err := decodeObject(record.Value, "envelope")
	if err != nil {
		return nil, err
	}
	op, err := stringField(envelope, "op", "envelope")
	if err != nil {
		return nil, err
	}
	if op != "c" && op != "r" {
		return nil, errorsf("unsupported operation")
	}
	if err := validateSource(envelope, source); err != nil {
		return nil, err
	}
	afterRaw, ok := envelope["after"]
	if !ok || isNull(afterRaw) {
		return nil, errorsf("operation has no after object")
	}
	after, err := decodeObject(afterRaw, "after")
	if err != nil {
		return nil, err
	}
	if err := validateKey(record.Key, after, identity); err != nil {
		return nil, err
	}
	return after, nil
}

// configuredValues converts every configured column, in identity order, and
// picks out the event id on the way.
func configuredValues(after map[string]json.RawMessage, identity stream.Identity) (map[string]json.RawMessage, string, error) {
	values := make(map[string]json.RawMessage, len(identity.Columns))
	var eventID string
	for _, column := range identity.Columns {
		converted, err := configuredValue(after, column, identity)
		if err != nil {
			return nil, "", err
		}
		values[column.Name] = converted
		if column.Name == identity.EventIDColumn {
			id, err := eventIDFrom(after[column.Name], column.Name)
			if err != nil {
				return nil, "", err
			}
			eventID = id
		}
	}
	return values, eventID, nil
}

// configuredValue converts one column: present, non-null when required, valid
// for its type.
func configuredValue(after map[string]json.RawMessage, column stream.Column, identity stream.Identity) (json.RawMessage, error) {
	raw, ok := after[column.Name]
	if !ok {
		return nil, errorsf("after object is missing configured column %q", column.Name)
	}
	if isNull(raw) && (column.Name == identity.SerialColumn || column.Name == identity.PartitioningColumn) {
		return nil, errorsf("required column %q is null", column.Name)
	}
	converted, err := protocol.ConvertValue(column.Type, raw, column.Name)
	if err != nil {
		return nil, errorsf("%v", err)
	}
	return converted, nil
}

// eventIDFrom decodes a non-null, non-empty event id.
func eventIDFrom(raw json.RawMessage, column string) (string, error) {
	if isNull(raw) {
		return "", errorsf("event id column %q is null", column)
	}
	eventID, err := decodeString(raw, "event id "+column)
	if err != nil {
		return "", err
	}
	if eventID == "" {
		return "", errorsf("event id column %q is empty", column)
	}
	return eventID, nil
}

// validateIdentity checks that identity is internally consistent and matches
// source: same table, same serial/partitioning columns, and configured
// columns that line up with source.Columns one for one.
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
	for i, column := range identity.Columns {
		if column.Name == "" || seen[column.Name] || source.Columns[i] != column.Name {
			return errorsf("identity has invalid configured columns")
		}
		seen[column.Name] = true
		if !column.Type.Supported() {
			return errorsf("configured column %q has unsupported type", column.Name)
		}
		if column.Name == identity.SerialColumn && !column.Type.Integer() {
			return errorsf("serial column %q has invalid type", column.Name)
		}
	}
	eventIDKnown := identity.EventIDColumn == "" || seen[identity.EventIDColumn]
	if !seen[identity.SerialColumn] || !seen[identity.PartitioningColumn] || !eventIDKnown {
		return errorsf("identity is missing a required configured column")
	}
	return nil
}

// validateSource checks the envelope's source metadata names the public
// schema and the configured table.
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

// validateKey checks the Kafka key holds only the partitioning column and
// that its value matches the same column in after.
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

// decodeObject decodes raw as a single JSON object, rejecting anything empty,
// non-object, or followed by trailing data.
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

// stringField reads a required string field named name out of object.
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

// compact re-encodes a JSON value without insignificant whitespace, so two
// equivalent values can be compared byte for byte.
func compact(raw []byte) ([]byte, error) {
	var out bytes.Buffer
	if err := json.Compact(&out, raw); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
func isNull(raw []byte) bool { return bytes.Equal(bytes.TrimSpace(raw), []byte("null")) }

func errorsf(format string, args ...any) error { return fmt.Errorf("payload: "+format, args...) }
