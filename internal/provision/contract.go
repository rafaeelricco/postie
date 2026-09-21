package provision

import (
	"errors"
	"fmt"
	"slices"

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
	Active    bool
	WALStatus WALStatus
	LagBytes  int64
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
