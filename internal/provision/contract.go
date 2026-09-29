package provision

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/rafaeelricco/postie/internal/stream"
)

// TableFacts is the catalog state read from a source table.
type TableFacts struct {
	Exists            bool
	Columns           map[string]ColumnFacts
	SerialUniqueIndex bool
}

// ColumnFacts is one column's PostgreSQL type and nullability.
type ColumnFacts struct {
	Type    stream.PGType
	NotNull bool
}

// ContractError means the source table breaks the capture contract. It is
// distinct from a connectivity error: the caller blocks the source on this
// one and retries on the other.
type ContractError struct{ Table, Reason string }

func (e *ContractError) Error() string { return "capture: table public." + e.Table + ": " + e.Reason }

// contractFor returns a constructor of ContractErrors for one table, so rule
// checks read as `return contract("serialColumn %q not found", name)`.
func contractFor(table string) func(format string, args ...any) error {
	return func(format string, args ...any) error {
		return &ContractError{Table: table, Reason: fmt.Sprintf(format, args...)}
	}
}

// isContract separates "the table is wrong" from "we could not find out".
func isContract(err error) bool {
	var contract *ContractError
	return errors.As(err, &contract)
}

// PublicationMode says who owns the publication.
type PublicationMode string

const (
	PublicationManaged  PublicationMode = "filtered"
	PublicationExternal PublicationMode = "disabled"
)

// ConnectorState is a Kafka Connect connector or task state.
type ConnectorState string

const (
	StateRunning    ConnectorState = "RUNNING"
	StateFailed     ConnectorState = "FAILED"
	StatePaused     ConnectorState = "PAUSED"
	StateUnassigned ConnectorState = "UNASSIGNED"
	StateRestarting ConnectorState = "RESTARTING"
)

// Status is a Debezium connector's status, as reported by Connect.
type Status struct {
	Connector ConnectorState
	Tasks     []ConnectorState
	Failed    bool
	Trace     string
}

// SlotStatus is the health of a PostgreSQL logical replication slot.
type SlotStatus struct {
	Exists    bool
	WALStatus WALStatus
}

// WALStatus is pg_replication_slots.wal_status.
type WALStatus string

const (
	WALReserved   WALStatus = "reserved"
	WALExtended   WALStatus = "extended"
	WALUnreserved WALStatus = "unreserved"
	WALLost       WALStatus = "lost"
)

// historyLost reports whether PostgreSQL has discarded WAL the slot still
// needed. Such a slot can never catch up, so the stream is unrecoverable.
func historyLost(slot SlotStatus) bool {
	return slot.WALStatus == WALLost || slot.WALStatus == WALUnreserved
}

// tasksRunning reports whether every task is RUNNING. It is true for no tasks,
// so callers that need at least one must check the length themselves.
func tasksRunning(tasks []ConnectorState) bool {
	return !slices.ContainsFunc(tasks, func(task ConnectorState) bool { return task != StateRunning })
}

// captureReady reports whether a new capture is fully up: a running connector
// with at least one task, all tasks running, and a slot that still has its WAL.
func captureReady(status Status, slot SlotStatus) bool {
	return status.Connector == StateRunning && len(status.Tasks) > 0 && !status.Failed &&
		tasksRunning(status.Tasks) && slot.Exists && !historyLost(slot)
}

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

// NamesFor derives every resource name of one stream generation. The names
// are persisted and compared on every health check, so this scheme must never
// change for an existing generation.
//
// The hash covers namespace, environment, source ID, and generation, which
// keeps distinct streams from colliding on a shared Kafka or PostgreSQL.
//
//	n := NamesFor("acme", "production", src, 2)
//	n.TopicPrefix // postie_g2_<28 hex chars>
//	n.Topic       // postie_g2_<28 hex chars>.public.<table>
//	n.Slot        // postie_g2_<28 hex chars>_slot
func NamesFor(namespace, environment string, src stream.Source, generation stream.Generation) stream.Names {
	// A JSON array is an unambiguous encoding: ("a", "bc") never hashes like ("ab", "c").
	identity, _ := json.Marshal([4]string{
		namespace, environment, src.ID, generation.String(),
	})
	sum := sha256.Sum256(identity)
	// Keep the slot name within PostgreSQL's 63-byte limit even at the maximum generation.
	prefix := fmt.Sprintf("postie_g%s_%s", generation.String(), hex.EncodeToString(sum[:14]))
	return stream.Names{
		TopicPrefix: prefix,
		Topic:       prefix + ".public." + src.Table,
		Connector:   prefix + "_connector",
		Slot:        prefix + "_slot",
		Publication: prefix + "_pub",
	}
}
