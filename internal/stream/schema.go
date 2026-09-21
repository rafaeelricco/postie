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
