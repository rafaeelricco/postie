package provision

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/rafaeelricco/postie/internal/stream"
)

var supportedColumnTypes = map[stream.PGType]bool{
	stream.PGInt8: true, stream.PGInt2: true, stream.PGInt4: true,
	stream.PGFloat4: true, stream.PGFloat8: true,
	stream.PGBool: true, stream.PGJSON: true, stream.PGBytea: true,
	stream.PGTimestamp: true, stream.PGTimestamptz: true, stream.PGText: true,
}

var serialColumnTypes = map[stream.PGType]bool{stream.PGInt2: true, stream.PGInt4: true, stream.PGInt8: true}

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

// IdentityFrom applies every capture contract rule to table facts and
// returns the frozen stream identity. It performs no I/O.
func IdentityFrom(src stream.Source, facts TableFacts, partitions int32) (stream.Identity, error) {
	if !facts.Exists {
		return stream.Identity{}, &ContractError{Table: src.Table, Reason: "does not exist"}
	}
	var missing []string
	columns := make([]stream.Column, 0, len(src.Columns))
	for _, name := range src.Columns {
		cf, ok := facts.Columns[name]
		if !ok {
			missing = append(missing, name)
			continue
		}
		if !supportedColumnTypes[cf.Type] {
			return stream.Identity{}, &ContractError{Table: src.Table, Reason: fmt.Sprintf("column %q has unsupported type %q", name, cf.Type)}
		}
		columns = append(columns, stream.Column{Name: name, Type: cf.Type})
	}
	if len(missing) > 0 {
		return stream.Identity{}, &ContractError{Table: src.Table, Reason: "missing configured columns: " + strings.Join(missing, ", ")}
	}
	serial, ok := facts.Columns[src.SerialColumn]
	if !ok {
		return stream.Identity{}, &ContractError{Table: src.Table, Reason: fmt.Sprintf("serialColumn %q not found", src.SerialColumn)}
	}
	if !serialColumnTypes[serial.Type] {
		return stream.Identity{}, &ContractError{Table: src.Table, Reason: fmt.Sprintf("serialColumn %q must be int2, int4, or int8, got %q", src.SerialColumn, serial.Type)}
	}
	if !serial.NotNull {
		return stream.Identity{}, &ContractError{Table: src.Table, Reason: fmt.Sprintf("serialColumn %q must be NOT NULL", src.SerialColumn)}
	}
	if !facts.SerialUniqueIndex {
		return stream.Identity{}, &ContractError{Table: src.Table, Reason: fmt.Sprintf("serialColumn %q has no unique or primary-key index on it alone", src.SerialColumn)}
	}
	partitioning, ok := facts.Columns[src.PartitioningColumn]
	if !ok {
		return stream.Identity{}, &ContractError{Table: src.Table, Reason: fmt.Sprintf("partitioningColumn %q not found", src.PartitioningColumn)}
	}
	if !partitioning.NotNull {
		return stream.Identity{}, &ContractError{Table: src.Table, Reason: fmt.Sprintf("partitioningColumn %q must be NOT NULL", src.PartitioningColumn)}
	}
	eventIDColumn := ""
	for _, name := range src.Columns {
		if name == "event_id" {
			eventIDColumn = "event_id"
			break
		}
	}
	return stream.Identity{Table: src.Table, SerialColumn: src.SerialColumn, PartitioningColumn: src.PartitioningColumn, EventIDColumn: eventIDColumn, Partitions: partitions, Columns: columns}, nil
}

func (s *Service) validateRegisteredStream(ctx context.Context, source stream.Source, identity stream.Identity, names stream.Names, registered stream.Registration) error {
	contract := func(reason string) error { return &ContractError{Table: source.Table, Reason: reason} }
	if !reflect.DeepEqual(registered.Identity, identity) {
		return contract("established identity differs from the source table")
	}
	if registered.Names != names {
		return contract("established capture names differ from configuration")
	}
	details, err := s.Topics.Topic(ctx, names.Topic)
	if err != nil {
		var metadata *TopicMetadataError
		if errors.As(err, &metadata) && metadata.Partition {
			return err
		}
		return fmt.Errorf("topic inspection unavailable")
	}
	if !details.Exists {
		return contract("established topic is missing")
	}
	if err := validateReplication(names.Topic, details, s.Replication); err != nil {
		return err
	}
	if details.ID != registered.TopicID {
		return contract("established topic has a different UUID")
	}
	if details.Partitions != int(identity.Partitions) {
		return contract("established partition count changed")
	}
	inspector := s.Sources[source.ID]
	slot, err := inspector.SlotHealth(ctx, names.Slot)
	if err != nil {
		return fmt.Errorf("slot inspection unavailable")
	}
	if !slot.Exists {
		return contract("established slot is missing")
	}
	if slot.WALStatus == WALLost || slot.WALStatus == WALUnreserved {
		return contract("established slot has lost WAL history")
	}
	status, err := s.Connectors.ConnectorStatus(ctx, names.Connector)
	if errors.Is(err, ErrConnectorMissing) {
		return contract("established connector is missing")
	}
	if err != nil || status.Failed {
		return fmt.Errorf("connector is unavailable")
	}
	return nil
}

func validateReplication(topic string, facts TopicFacts, replication int16) error {
	partitions := make([]int, 0, len(facts.Replicas))
	for partition := range facts.Replicas {
		partitions = append(partitions, int(partition))
	}
	sort.Ints(partitions)
	for _, partition := range partitions {
		count := facts.Replicas[int32(partition)]
		if count != int(replication) {
			return fmt.Errorf("capture: topic %q partition %d has %d replicas, want %d", topic, partition, count, replication)
		}
	}
	return nil
}
