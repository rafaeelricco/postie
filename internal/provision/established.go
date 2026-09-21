package provision

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/rafaeelricco/postie/internal/stream"
)

// validateRegisteredStream checks that everything a registration points at
// still exists and still matches. A *ContractError means the stream is
// broken for good; any other error means a dependency could not answer.
func (s *Service) validateRegisteredStream(ctx context.Context, source stream.Source, identity stream.Identity, names stream.Names, registered stream.Registration) error {
	contract := contractFor(source.Table)
	if !reflect.DeepEqual(registered.Identity, identity) {
		return contract("established identity differs from the source table")
	}
	if registered.Names != names {
		return contract("established capture names differ from configuration")
	}
	if err := s.validateTopic(ctx, source, identity, names, registered); err != nil {
		return err
	}
	if err := s.validateSlot(ctx, source, names); err != nil {
		return err
	}
	return s.validateConnector(ctx, source, names)
}

// validateTopic checks the topic exists, is fully replicated, and still has
// the UUID and partition count that were established.
func (s *Service) validateTopic(ctx context.Context, source stream.Source, identity stream.Identity, names stream.Names, registered stream.Registration) error {
	contract := contractFor(source.Table)
	details, err := s.Topics.Topic(ctx, names.Topic)
	if err != nil {
		// A partition-level error names the broken partition; keep it.
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
	return nil
}

// validateSlot checks the replication slot exists and has not lost WAL
// history.
func (s *Service) validateSlot(ctx context.Context, source stream.Source, names stream.Names) error {
	contract := contractFor(source.Table)
	slot, err := s.Sources[source.ID].SlotHealth(ctx, names.Slot)
	if err != nil {
		return fmt.Errorf("slot inspection unavailable")
	}
	if !slot.Exists {
		return contract("established slot is missing")
	}
	if historyLost(slot) {
		return contract("established slot has lost WAL history")
	}
	return nil
}

// validateConnector checks the Debezium connector exists and has not failed.
func (s *Service) validateConnector(ctx context.Context, source stream.Source, names stream.Names) error {
	status, err := s.Connectors.ConnectorStatus(ctx, names.Connector)
	if errors.Is(err, ErrConnectorMissing) {
		return contractFor(source.Table)("established connector is missing")
	}
	if err != nil || status.Failed {
		return fmt.Errorf("connector is unavailable")
	}
	return nil
}

// validateReplication checks every partition's replica count, reporting the
// lowest mismatching partition so the message is stable between runs.
func validateReplication(topic string, facts TopicFacts, replication int16) error {
	partitions := make([]int32, 0, len(facts.Replicas))
	for partition := range facts.Replicas {
		partitions = append(partitions, partition)
	}
	slices.Sort(partitions)
	for _, partition := range partitions {
		if count := facts.Replicas[partition]; count != int(replication) {
			return fmt.Errorf("capture: topic %q partition %d has %d replicas, want %d", topic, partition, count, replication)
		}
	}
	return nil
}
