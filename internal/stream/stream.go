package stream

import (
	"encoding/json"
	"strconv"
)

// Identity is frozen per stream generation (spec §Capture contract). It is
// stored as JSONB in the control store, so its fields are JSON-tagged.
type Identity struct {
	Table              string   `json:"table"`
	SerialColumn       string   `json:"serialColumn"`
	PartitioningColumn string   `json:"partitioningColumn"`
	EventIDColumn      string   `json:"eventIdColumn"`
	Partitions         int32    `json:"partitions"`
	Columns            []Column `json:"columns"` // in config order; the payload profile input
}

// Names are the deterministic Kafka/Postgres/Connect names for one stream
// generation. TopicPrefix is the Debezium topic.prefix; since Debezium
// names topics "<topic.prefix>.<schema>.<table>", Topic is the full Kafka
// topic name Debezium actually writes to.
type Names struct {
	TopicPrefix string
	Topic       string
	Connector   string
	Slot        string
	Publication string
}

// Scope identifies one deployed stream generation.
type Scope struct {
	Namespace   string
	Environment string
	Generation  Generation
}

// Registration is the control-plane registration for one source stream.
type Registration struct {
	SourceID string
	Identity Identity
	Names    Names
	TopicID  [16]byte
	Blocked  string
}

// Generation numbers a stream generation. Zero is invalid; the first is 1.
type Generation int

func (g Generation) Valid() bool    { return g >= 1 }
func (g Generation) String() string { return strconv.Itoa(int(g)) }

// Record is the decoded event delivered to destinations.
type Record struct {
	Source     Source
	Payload    json.RawMessage
	Topic      string
	Partition  int32
	Offset     int64
	EventID    string
	Generation Generation
	Replay     bool
}

// RawRecord is the broker-level event before payload decoding.
type RawRecord struct {
	Topic       string
	Partition   int32
	Offset      int64
	LeaderEpoch int32
	Key         []byte
	Value       []byte
}

// Source is the capture-facing source description shared by engine packages.
// It contains no credentials or connection details.
type Source struct {
	ID                 string
	Description        string
	Table              string
	Columns            []string
	SerialColumn       string
	PartitioningColumn string
}

// Column is one configured column of the source table, as reported by
// Postgres: Name is the column name and Type is pg_type.typname.
type Column struct {
	Name string `json:"name"`
	Type PGType `json:"type"`
}

// PGType is a PostgreSQL type name as reported by pg_type.typname.
type PGType string

const (
	PGInt2        PGType = "int2"
	PGInt4        PGType = "int4"
	PGInt8        PGType = "int8"
	PGFloat4      PGType = "float4"
	PGFloat8      PGType = "float8"
	PGBool        PGType = "bool"
	PGJSON        PGType = "json"
	PGBytea       PGType = "bytea"
	PGTimestamp   PGType = "timestamp"
	PGTimestamptz PGType = "timestamptz"
	PGText        PGType = "text"
)

// Supported reports whether Postie can capture and convert a column of this
// type. Provisioning and decoding both ask here, so the two can never disagree
// about which tables are acceptable.
//
//	stream.PGJSON.Supported()         // true
//	stream.PGType("uuid").Supported() // false
func (t PGType) Supported() bool {
	switch t {
	case PGInt2, PGInt4, PGInt8, PGFloat4, PGFloat8, PGBool, PGJSON, PGBytea, PGTimestamp, PGTimestamptz, PGText:
		return true
	default:
		return false
	}
}

// Integer reports whether the type can order rows as a serial column.
func (t PGType) Integer() bool { return t == PGInt2 || t == PGInt4 || t == PGInt8 }
