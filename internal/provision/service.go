package provision

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/rafaeelricco/postie/internal/stream"
)

// Service provisions and health-checks source streams through explicit
// infrastructure adapters.
type Service struct {
	Scope        stream.Scope
	Partitions   int32
	Replication  int16
	Publications map[string]string
	Sources      map[string]Source
	Topics       Topics
	Connectors   Connectors
	Store        Store
}

// ProvisionSource ensures the table identity, topic, connector, slot, and
// control-store registration for one source.
func (s *Service) ProvisionSource(ctx context.Context, source stream.Source) (stream.Registration, error) {
	inspector := s.Sources[source.ID]
	facts, err := inspector.InspectTable(ctx)
	if err != nil {
		return stream.Registration{}, err
	}
	identity, err := IdentityFrom(source, facts, s.Partitions)
	if err != nil {
		return stream.Registration{}, err
	}

	names := NamesFor(s.Scope.Namespace, s.Scope.Environment, source, s.Scope.Generation)
	publication := PublicationManaged
	if external := s.Publications[source.ID]; external != "" {
		names.Publication = external
		publication = PublicationExternal
	}

	registered, found, err := s.Store.GetStream(ctx, s.Scope, source.ID)
	if err != nil {
		return stream.Registration{}, err
	}
	if found {
		if registered.Blocked != "" {
			return stream.Registration{}, fmt.Errorf("control: source is blocked: %s", registered.Blocked)
		}
		if err := s.validateRegisteredStream(ctx, source, identity, names, registered); err != nil {
			var contract *ContractError
			if !errors.As(err, &contract) {
				return stream.Registration{}, fmt.Errorf("capture health could not be verified; retry provisioning: %w", err)
			}
			reason := err.Error()
			if blockErr := s.Store.BlockSource(ctx, s.Scope, source.ID, reason); blockErr != nil {
				return stream.Registration{}, fmt.Errorf("%v; block source: %w", err, blockErr)
			}
			return stream.Registration{}, fmt.Errorf("control: source blocked: %s", reason)
		}
		return registered, nil
	}

	topicID, err := s.Topics.EnsureTopic(ctx, names, identity.Partitions, s.Replication)
	if err != nil {
		return stream.Registration{}, err
	}
	if err := s.Connectors.EnsureConnector(ctx, source, identity, names, publication); err != nil {
		return stream.Registration{}, err
	}
	for {
		status, statusErr := s.Connectors.ConnectorStatus(ctx, names.Connector)
		slot, slotErr := inspector.SlotHealth(ctx, names.Slot)
		ready := statusErr == nil && slotErr == nil && status.Connector == StateRunning && len(status.Tasks) > 0 && !status.Failed && slot.Exists && slot.WALStatus != WALLost && slot.WALStatus != WALUnreserved
		for _, task := range status.Tasks {
			ready = ready && task == StateRunning
		}
		if ready {
			break
		}
		select {
		case <-ctx.Done():
			return stream.Registration{}, fmt.Errorf("capture did not become ready before provisioning timed out")
		case <-time.After(200 * time.Millisecond):
		}
	}
	registered = stream.Registration{SourceID: source.ID, Identity: identity, Names: names, TopicID: topicID}
	if err := s.Store.RegisterStream(ctx, s.Scope, registered); err != nil {
		return stream.Registration{}, err
	}
	return registered, nil
}

// InspectSource checks the established stream health. It returns the state
// and user-facing reason; durable blocking is owned by control.
func (s *Service) InspectSource(ctx context.Context, source stream.Source, registered stream.Registration) (string, string) {
	inspector := s.Sources[source.ID]
	identity, err := inspector.InspectTable(ctx)
	if err != nil {
		var contract *ContractError
		if errors.As(err, &contract) {
			return "blocked", "source table violates capture contract"
		}
		return "unavailable", "source database unavailable"
	}
	frozen, err := IdentityFrom(source, identity, s.Partitions)
	if err != nil {
		var contract *ContractError
		if errors.As(err, &contract) {
			return "blocked", "source table violates capture contract"
		}
		return "unavailable", "source database unavailable"
	}
	if !reflect.DeepEqual(frozen, registered.Identity) {
		return "blocked", "stream identity changed"
	}
	names := NamesFor(s.Scope.Namespace, s.Scope.Environment, source, s.Scope.Generation)
	if publication := s.Publications[source.ID]; publication != "" {
		names.Publication = publication
	}
	if registered.Names != names {
		return "blocked", "stream configuration changed"
	}
	details, err := s.Topics.Topic(ctx, names.Topic)
	if err != nil {
		var metadata *TopicMetadataError
		if errors.As(err, &metadata) {
			if metadata.Partition {
				return "unavailable", metadata.Error()
			}
			return "unavailable", "Kafka topic unavailable"
		}
		return "unavailable", "Kafka metadata unavailable"
	}
	if !details.Exists {
		return "blocked", "established topic missing"
	}
	if err := validateReplication(names.Topic, details, s.Replication); err != nil {
		return "unavailable", err.Error()
	}
	if details.ID != registered.TopicID || details.Partitions != int(registered.Identity.Partitions) {
		return "blocked", "topic identity changed"
	}
	slot, err := inspector.SlotHealth(ctx, names.Slot)
	if err != nil {
		return "unavailable", "replication slot health unavailable"
	}
	if !slot.Exists || slot.WALStatus == WALLost || slot.WALStatus == WALUnreserved {
		return "blocked", "replication slot or WAL history lost"
	}
	status, err := s.Connectors.ConnectorStatus(ctx, names.Connector)
	if errors.Is(err, ErrConnectorMissing) {
		return "blocked", "established connector missing"
	}
	if err != nil || status.Failed || status.Connector != StateRunning || len(status.Tasks) == 0 {
		return "unavailable", "capture connector unavailable"
	}
	for _, task := range status.Tasks {
		if task != StateRunning {
			return "unavailable", "capture task unavailable"
		}
	}
	return "running", ""
}
