package provision

import (
	"context"
	"errors"
	"reflect"

	"github.com/rafaeelricco/postie/internal/stream"
)

// health is a source's condition, as InspectSource reports it.
type health string

const (
	healthRunning     health = "running"
	healthBlocked     health = "blocked"     // broken for good; needs an operator
	healthUnavailable health = "unavailable" // a dependency could not answer; retry
)

// finding is what one inspection step found wrong. The zero value means
// "nothing wrong", and blocked and unavailable are the only other ways to
// make one, so a finding is never a reason without a state, and never
// "running" with a reason.
type finding struct {
	health health
	reason string
}

func blocked(reason string) finding     { return finding{healthBlocked, reason} }
func unavailable(reason string) finding { return finding{healthUnavailable, reason} }

// found reports whether the step has something to report.
func (f finding) found() bool { return f.health != "" }

// InspectSource checks the established stream health. It returns the state
// and user-facing reason; durable blocking is owned by control.
//
// The checks run in order and the first finding wins. "blocked" means the
// stream can no longer be trusted; "unavailable" means try again later.
//
//	state, reason := service.InspectSource(ctx, source, registered)
//	// "running", ""
//	// "blocked", "replication slot or WAL history lost"
//	// "unavailable", "Kafka metadata unavailable"
func (s *Service) InspectSource(ctx context.Context, source stream.Source, registered stream.Registration) (state, reason string) {
	names, _ := s.captureNames(source)
	checks := []func() finding{
		func() finding { return s.inspectIdentity(ctx, source, registered) },
		func() finding { return inspectNames(registered, names) },
		func() finding { return s.inspectTopic(ctx, names, registered) },
		func() finding { return s.inspectSlot(ctx, source, names) },
		func() finding { return s.inspectConnector(ctx, names) },
	}
	for _, check := range checks {
		if f := check(); f.found() {
			return string(f.health), f.reason
		}
	}
	return string(healthRunning), ""
}

// inspectIdentity checks the source table still matches the identity that was
// established: same columns, same partitioning.
func (s *Service) inspectIdentity(ctx context.Context, source stream.Source, registered stream.Registration) finding {
	facts, err := s.Sources[source.ID].InspectTable(ctx)
	if err != nil {
		return tableFailure(err)
	}
	identity, err := IdentityFrom(source, facts, s.Partitions)
	if err != nil {
		return tableFailure(err)
	}
	if !reflect.DeepEqual(identity, registered.Identity) {
		return blocked("stream identity changed")
	}
	return finding{}
}

// tableFailure classifies a table inspection error as blocked (violates the
// capture contract) or unavailable (the database could not be reached).
func tableFailure(err error) finding {
	if isContract(err) {
		return blocked("source table violates capture contract")
	}
	return unavailable("source database unavailable")
}

// inspectNames checks the derived capture names still match what was
// established.
func inspectNames(registered stream.Registration, names stream.Names) finding {
	if registered.Names != names {
		return blocked("stream configuration changed")
	}
	return finding{}
}

// inspectTopic checks the topic is the one that was registered: same UUID,
// same partition count, full replication.
func (s *Service) inspectTopic(ctx context.Context, names stream.Names, registered stream.Registration) finding {
	details, err := s.Topics.Topic(ctx, names.Topic)
	if err != nil {
		return unavailable(topicFailure(err))
	}
	if !details.Exists {
		return blocked("established topic missing")
	}
	if err := validateReplication(names.Topic, details, s.Replication); err != nil {
		return unavailable(err.Error())
	}
	if details.ID != registered.TopicID || details.Partitions != int(registered.Identity.Partitions) {
		return blocked("topic identity changed")
	}
	return finding{}
}

// topicFailure keeps a partition-level diagnostic and generalizes the rest.
func topicFailure(err error) string {
	var metadata *TopicMetadataError
	switch {
	case !errors.As(err, &metadata):
		return "Kafka metadata unavailable"
	case metadata.Partition:
		return metadata.Error()
	default:
		return "Kafka topic unavailable"
	}
}

// inspectSlot checks the replication slot still exists and has not lost WAL
// history.
func (s *Service) inspectSlot(ctx context.Context, source stream.Source, names stream.Names) finding {
	slot, err := s.Sources[source.ID].SlotHealth(ctx, names.Slot)
	if err != nil {
		return unavailable("replication slot health unavailable")
	}
	if !slot.Exists || historyLost(slot) {
		return blocked("replication slot or WAL history lost")
	}
	return finding{}
}

// inspectConnector checks the Debezium connector exists, is running, and has
// running tasks.
func (s *Service) inspectConnector(ctx context.Context, names stream.Names) finding {
	status, err := s.Connectors.ConnectorStatus(ctx, names.Connector)
	if errors.Is(err, ErrConnectorMissing) {
		return blocked("established connector missing")
	}
	if err != nil || status.Failed || status.Connector != StateRunning || len(status.Tasks) == 0 {
		return unavailable("capture connector unavailable")
	}
	if !tasksRunning(status.Tasks) {
		return unavailable("capture task unavailable")
	}
	return finding{}
}
