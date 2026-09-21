package provision

import (
	"slices"
	"strings"

	"github.com/rafaeelricco/postie/internal/stream"
)

// eventIDColumnName is the optional column that carries a stable event ID.
const eventIDColumnName = "event_id"

// IdentityFrom applies every capture contract rule to table facts and
// returns the frozen stream identity. It performs no I/O. A broken rule is
// reported as a *ContractError.
//
//	facts, _ := source.InspectTable(ctx)
//	identity, err := provision.IdentityFrom(src, facts, 10)
//	// err: capture: table public.events: serialColumn "id" must be NOT NULL
func IdentityFrom(src stream.Source, facts TableFacts, partitions int32) (stream.Identity, error) {
	if !facts.Exists {
		return stream.Identity{}, contractFor(src.Table)("does not exist")
	}
	columns, err := configuredColumns(src, facts)
	if err != nil {
		return stream.Identity{}, err
	}
	if err := checkSerialColumn(src, facts); err != nil {
		return stream.Identity{}, err
	}
	if err := checkPartitioningColumn(src, facts); err != nil {
		return stream.Identity{}, err
	}
	return stream.Identity{
		Table:              src.Table,
		SerialColumn:       src.SerialColumn,
		PartitioningColumn: src.PartitioningColumn,
		EventIDColumn:      eventIDColumn(src.Columns),
		Partitions:         partitions,
		Columns:            columns,
	}, nil
}

// configuredColumns resolves the configured column names, in config order,
// to their PostgreSQL types. All missing columns are reported together.
func configuredColumns(src stream.Source, facts TableFacts) ([]stream.Column, error) {
	contract := contractFor(src.Table)
	var missing []string
	columns := make([]stream.Column, 0, len(src.Columns))
	for _, name := range src.Columns {
		column, ok := facts.Columns[name]
		if !ok {
			missing = append(missing, name)
			continue
		}
		if !column.Type.Supported() {
			return nil, contract("column %q has unsupported type %q", name, column.Type)
		}
		columns = append(columns, stream.Column{Name: name, Type: column.Type})
	}
	if len(missing) > 0 {
		return nil, contract("missing configured columns: %s", strings.Join(missing, ", "))
	}
	return columns, nil
}

// checkSerialColumn requires a NOT NULL integer with its own unique index:
// the snapshot is ordered by it, so it must identify and order every row.
func checkSerialColumn(src stream.Source, facts TableFacts) error {
	contract := contractFor(src.Table)
	serial, ok := facts.Columns[src.SerialColumn]
	switch {
	case !ok:
		return contract("serialColumn %q not found", src.SerialColumn)
	case !serial.Type.Integer():
		return contract("serialColumn %q must be int2, int4, or int8, got %q", src.SerialColumn, serial.Type)
	case !serial.NotNull:
		return contract("serialColumn %q must be NOT NULL", src.SerialColumn)
	case !facts.SerialUniqueIndex:
		return contract("serialColumn %q has no unique or primary-key index on it alone", src.SerialColumn)
	default:
		return nil
	}
}

// checkPartitioningColumn requires NOT NULL because the value becomes the
// Kafka message key, and a null key would lose per-key ordering.
func checkPartitioningColumn(src stream.Source, facts TableFacts) error {
	contract := contractFor(src.Table)
	partitioning, ok := facts.Columns[src.PartitioningColumn]
	switch {
	case !ok:
		return contract("partitioningColumn %q not found", src.PartitioningColumn)
	case !partitioning.NotNull:
		return contract("partitioningColumn %q must be NOT NULL", src.PartitioningColumn)
	default:
		return nil
	}
}

// eventIDColumn is "event_id" when that column is configured, otherwise "".
func eventIDColumn(columns []string) string {
	if slices.Contains(columns, eventIDColumnName) {
		return eventIDColumnName
	}
	return ""
}
