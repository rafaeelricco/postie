package provision

import (
	"context"
	"errors"

	"github.com/rafaeelricco/postie/internal/stream"
)

// ErrConnectorMissing identifies a connector that Connect does not know.
var ErrConnectorMissing = errors.New("capture: connector missing")

// Source abstracts catalog and replication-slot inspection for one source.
type Source interface {
	InspectTable(context.Context) (TableFacts, error)
	SlotHealth(context.Context, string) (SlotStatus, error)
}

// Topics abstracts Kafka topic provisioning and inspection.
type Topics interface {
	EnsureTopic(context.Context, stream.Names, int32, int16) ([16]byte, error)
	Topic(context.Context, string) (TopicFacts, error)
}

// TopicFacts is the Kafka metadata needed by provisioning validation.
type TopicFacts struct {
	Exists     bool
	ID         [16]byte
	Partitions int
	Replicas   map[int32]int
}

// TopicMetadataError distinguishes a topic or partition response error from a
// failure to fetch broker metadata. The distinction preserves diagnostics.
type TopicMetadataError struct {
	Partition bool
	Cause     error
}

func (e *TopicMetadataError) Error() string { return e.Cause.Error() }

// Connectors abstracts Kafka Connect provisioning and status inspection.
type Connectors interface {
	EnsureConnector(context.Context, stream.Source, stream.Identity, stream.Names, PublicationMode) error
	ConnectorStatus(context.Context, string) (Status, error)
}

// Store is the control-plane persistence needed by provisioning.
type Store interface {
	GetStream(context.Context, stream.Scope, string) (stream.Registration, bool, error)
	RegisterStream(context.Context, stream.Scope, stream.Registration) error
	BlockSource(context.Context, stream.Scope, string, string) error
}
