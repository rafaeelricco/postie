package provision

import (
	"context"
	"fmt"
	"time"

	"github.com/rafaeelricco/postie/internal/stream"
)

// captureReadyPoll is how often a new capture is checked for readiness.
const captureReadyPoll = 200 * time.Millisecond

// Service provisions and health-checks source streams through explicit
// infrastructure adapters.
type Service struct {
	Scope        stream.Scope
	Partitions   int32
	Replication  int16
	Publications map[string]string // source ID -> externally owned publication
	Sources      map[string]Source
	Topics       Topics
	Connectors   Connectors
	Store        Store
}

// ProvisionSource ensures the table identity, topic, connector, slot, and
// control-store registration for one source. It is idempotent: a stream that
// is already registered is only verified, never recreated.
//
//	registered, err := service.ProvisionSource(ctx, source)
//	// registered.Names.Topic is where Debezium writes this source's events
func (s *Service) ProvisionSource(ctx context.Context, source stream.Source) (stream.Registration, error) {
	facts, err := s.Sources[source.ID].InspectTable(ctx)
	if err != nil {
		return stream.Registration{}, err
	}
	identity, err := IdentityFrom(source, facts, s.Partitions)
	if err != nil {
		return stream.Registration{}, err
	}
	names, publication := s.captureNames(source)
	registered, found, err := s.Store.GetStream(ctx, s.Scope, source.ID)
	if err != nil {
		return stream.Registration{}, err
	}
	if found {
		return s.confirmEstablished(ctx, source, identity, names, registered)
	}
	return s.establish(ctx, source, identity, names, publication)
}

// captureNames derives the stream's resource names. A source with an
// external publication uses that name and tells Debezium not to create one.
func (s *Service) captureNames(source stream.Source) (stream.Names, PublicationMode) {
	names := NamesFor(s.Scope.Namespace, s.Scope.Environment, source, s.Scope.Generation)
	if external := s.Publications[source.ID]; external != "" {
		names.Publication = external
		return names, PublicationExternal
	}
	return names, PublicationManaged
}

// confirmEstablished verifies a registered stream against the live systems.
// A contract violation blocks the source durably. Anything else is treated as
// transient, and the caller is asked to retry.
func (s *Service) confirmEstablished(ctx context.Context, source stream.Source, identity stream.Identity, names stream.Names, registered stream.Registration) (stream.Registration, error) {
	if registered.Blocked != "" {
		return stream.Registration{}, fmt.Errorf("control: source is blocked: %s", registered.Blocked)
	}
	err := s.validateRegisteredStream(ctx, source, identity, names, registered)
	if err == nil {
		return registered, nil
	}
	if !isContract(err) {
		return stream.Registration{}, fmt.Errorf("capture health could not be verified; retry provisioning: %w", err)
	}
	reason := err.Error()
	if blockErr := s.Store.BlockSource(ctx, s.Scope, source.ID, reason); blockErr != nil {
		return stream.Registration{}, fmt.Errorf("%v; block source: %w", err, blockErr)
	}
	return stream.Registration{}, fmt.Errorf("control: source blocked: %s", reason)
}

// establish creates the topic and connector, waits for the capture to come
// up, and registers the stream last, so a registration always points at a
// capture that worked at least once.
func (s *Service) establish(ctx context.Context, source stream.Source, identity stream.Identity, names stream.Names, publication PublicationMode) (stream.Registration, error) {
	topicID, err := s.Topics.EnsureTopic(ctx, names, identity.Partitions, s.Replication)
	if err != nil {
		return stream.Registration{}, err
	}
	if err := s.Connectors.EnsureConnector(ctx, source, identity, names, publication); err != nil {
		return stream.Registration{}, err
	}
	if err := s.awaitCapture(ctx, s.Sources[source.ID], names); err != nil {
		return stream.Registration{}, err
	}
	registered := stream.Registration{SourceID: source.ID, Identity: identity, Names: names, TopicID: topicID}
	if err := s.Store.RegisterStream(ctx, s.Scope, registered); err != nil {
		return stream.Registration{}, err
	}
	return registered, nil
}

// awaitCapture polls until the connector and its slot are ready. Errors while
// polling are expected during startup, so only ctx ends the wait.
func (s *Service) awaitCapture(ctx context.Context, inspector Source, names stream.Names) error {
	for {
		status, statusErr := s.Connectors.ConnectorStatus(ctx, names.Connector)
		slot, slotErr := inspector.SlotHealth(ctx, names.Slot)
		if statusErr == nil && slotErr == nil && captureReady(status, slot) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("capture did not become ready before provisioning timed out")
		case <-time.After(captureReadyPoll):
		}
	}
}
