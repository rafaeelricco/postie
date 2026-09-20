package stream

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
