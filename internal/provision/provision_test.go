package provision

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/stream"
)

type serviceSourceFake struct {
	mu          sync.Mutex
	facts       TableFacts
	inspectErr  error
	slot        SlotStatus
	slotErr     error
	inspectCall int
	slotCalls   []string
}

func (f *serviceSourceFake) InspectTable(context.Context) (TableFacts, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inspectCall++
	return f.facts, f.inspectErr
}

func (f *serviceSourceFake) SlotHealth(_ context.Context, slot string) (SlotStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.slotCalls = append(f.slotCalls, slot)
	return f.slot, f.slotErr
}

type serviceTopicsFake struct {
	mu          sync.Mutex
	topicID     [16]byte
	ensureErr   error
	topicFacts  TopicFacts
	topicErr    error
	ensureCalls int
	topicCalls  []string
	ensureNames stream.Names
	ensureParts int32
	ensureRepl  int16
}

func (f *serviceTopicsFake) EnsureTopic(_ context.Context, names stream.Names, partitions int32, replication int16) ([16]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureCalls++
	f.ensureNames, f.ensureParts, f.ensureRepl = names, partitions, replication
	return f.topicID, f.ensureErr
}

func (f *serviceTopicsFake) Topic(_ context.Context, name string) (TopicFacts, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.topicCalls = append(f.topicCalls, name)
	return f.topicFacts, f.topicErr
}

type serviceConnectorsFake struct {
	mu             sync.Mutex
	ensureErr      error
	status         Status
	statusErr      error
	statusSequence []Status
	statusErrors   []error
	ensureCalls    int
	statusCalls    []string
	ensureSource   stream.Source
	ensureIdentity stream.Identity
	ensureNames    stream.Names
	ensureMode     PublicationMode
}

func (f *serviceConnectorsFake) EnsureConnector(_ context.Context, source stream.Source, identity stream.Identity, names stream.Names, mode PublicationMode) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureCalls++
	f.ensureSource, f.ensureIdentity, f.ensureNames, f.ensureMode = source, identity, names, mode
	return f.ensureErr
}

func (f *serviceConnectorsFake) ConnectorStatus(_ context.Context, name string) (Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statusCalls = append(f.statusCalls, name)
	if len(f.statusSequence) > 0 {
		status := f.statusSequence[0]
		f.statusSequence = f.statusSequence[1:]
		var err error
		if len(f.statusErrors) > 0 {
			err = f.statusErrors[0]
			f.statusErrors = f.statusErrors[1:]
		}
		return status, err
	}
	return f.status, f.statusErr
}

type serviceStoreFake struct {
	mu            sync.Mutex
	registered    stream.Registration
	found         bool
	getErr        error
	registerErr   error
	blockErr      error
	getCalls      int
	registerCalls int
	blockCalls    []string
}

func (f *serviceStoreFake) GetStream(context.Context, stream.Scope, string) (stream.Registration, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	return f.registered, f.found, f.getErr
}

func (f *serviceStoreFake) RegisterStream(_ context.Context, _ stream.Scope, registered stream.Registration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registerCalls++
	if f.registerErr == nil {
		f.registered = registered
		f.found = true
	}
	return f.registerErr
}

func (f *serviceStoreFake) BlockSource(_ context.Context, _ stream.Scope, source, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blockCalls = append(f.blockCalls, source+":"+reason)
	return f.blockErr
}

func serviceFacts() TableFacts {
	return TableFacts{Exists: true, SerialUniqueIndex: true, Columns: map[string]ColumnFacts{
		"id":             {Type: stream.PGInt8, NotNull: true},
		"correlation_id": {Type: stream.PGText, NotNull: true},
		"payload":        {Type: stream.PGText, NotNull: true},
	}}
}

func serviceIdentity(src stream.Source, partitions int32) stream.Identity {
	return stream.Identity{
		Table: src.Table, SerialColumn: src.SerialColumn, PartitioningColumn: src.PartitioningColumn,
		Partitions: partitions,
		Columns:    []stream.Column{{Name: "id", Type: stream.PGInt8}, {Name: "correlation_id", Type: stream.PGText}, {Name: "payload", Type: stream.PGText}},
	}
}

func testSource() stream.Source {
	return stream.Source{ID: "orders", Description: "orders db", Table: "event_store", Columns: []string{"id", "correlation_id", "payload"}, SerialColumn: "id", PartitioningColumn: "correlation_id"}
}

func newProvisionService(t *testing.T) (*Service, stream.Source, *serviceSourceFake, *serviceTopicsFake, *serviceConnectorsFake, *serviceStoreFake) {
	t.Helper()
	source := testSource()
	sourceFake := &serviceSourceFake{facts: serviceFacts(), slot: SlotStatus{Exists: true, WALStatus: WALReserved}}
	topics := &serviceTopicsFake{topicID: [16]byte{1, 2, 3}, topicFacts: TopicFacts{Exists: true, ID: [16]byte{1, 2, 3}, Partitions: 3, Replicas: map[int32]int{0: 2, 1: 2, 2: 2}}}
	connectors := &serviceConnectorsFake{status: Status{Connector: StateRunning, Tasks: []ConnectorState{StateRunning}}}
	store := &serviceStoreFake{}
	service := &Service{
		Scope:      stream.Scope{Namespace: "postie", Environment: "test", Generation: 1},
		Partitions: 3, Replication: 2,
		Sources: map[string]Source{source.ID: sourceFake},
		Topics:  topics, Connectors: connectors, Store: store,
		Publications: map[string]string{},
	}
	return service, source, sourceFake, topics, connectors, store
}

func registeredFor(service *Service, source stream.Source, topicID [16]byte) stream.Registration {
	identity := serviceIdentity(source, service.Partitions)
	return stream.Registration{SourceID: source.ID, Identity: identity, Names: NamesFor(service.Scope.Namespace, service.Scope.Environment, source, service.Scope.Generation), TopicID: topicID}
}

func TestProvisionSourceUnregisteredCreatesDurableRegistration(t *testing.T) {
	service, source, _, topics, connectors, store := newProvisionService(t)
	got, err := service.ProvisionSource(context.Background(), source)
	if err != nil {
		t.Fatalf("ProvisionSource: %v", err)
	}
	wantNames := NamesFor(service.Scope.Namespace, service.Scope.Environment, source, service.Scope.Generation)
	if got.SourceID != source.ID || got.TopicID != topics.topicID || !reflect.DeepEqual(got.Names, wantNames) || !reflect.DeepEqual(got.Identity, serviceIdentity(source, service.Partitions)) {
		t.Fatalf("registration = %+v", got)
	}
	if topics.ensureCalls != 1 || topics.ensureNames != wantNames || topics.ensureParts != 3 || topics.ensureRepl != 2 {
		t.Fatalf("EnsureTopic calls=%d names=%+v partitions=%d replication=%d", topics.ensureCalls, topics.ensureNames, topics.ensureParts, topics.ensureRepl)
	}
	if connectors.ensureCalls != 1 || connectors.ensureMode != PublicationManaged || connectors.ensureNames != wantNames || !reflect.DeepEqual(connectors.ensureIdentity, got.Identity) {
		t.Fatalf("EnsureConnector calls=%d mode=%q names=%+v", connectors.ensureCalls, connectors.ensureMode, connectors.ensureNames)
	}
	if store.registerCalls != 1 || len(store.blockCalls) != 0 {
		t.Fatalf("store register=%d block=%v", store.registerCalls, store.blockCalls)
	}
}

func TestProvisionSourceExternalPublicationUsesConfiguredName(t *testing.T) {
	service, source, _, _, connectors, store := newProvisionService(t)
	service.Publications[source.ID] = "external_publication"
	got, err := service.ProvisionSource(context.Background(), source)
	if err != nil {
		t.Fatalf("ProvisionSource: %v", err)
	}
	if got.Names.Publication != "external_publication" || connectors.ensureMode != PublicationExternal || store.registered.Names.Publication != "external_publication" {
		t.Fatalf("external publication registration=%+v mode=%q", got, connectors.ensureMode)
	}
}

func TestProvisionSourcePollsUntilConnectorAndSlotReady(t *testing.T) {
	service, source, _, _, connectors, store := newProvisionService(t)
	connectors.statusSequence = []Status{
		{Connector: StateRestarting, Tasks: []ConnectorState{StateUnassigned}},
		{Connector: StateRunning, Tasks: []ConnectorState{}},
		{Connector: StateRunning, Tasks: []ConnectorState{StateRunning}},
	}
	got, err := service.ProvisionSource(context.Background(), source)
	if err != nil || got.SourceID != source.ID || store.registerCalls != 1 || len(connectors.statusCalls) != 3 {
		t.Fatalf("polling result=%+v err=%v register=%d status calls=%d", got, err, store.registerCalls, len(connectors.statusCalls))
	}
}

func TestProvisionSourceRegisteredValidation(t *testing.T) {
	contractCases := []struct {
		name   string
		mutate func(*Service, stream.Source, *serviceSourceFake, *serviceTopicsFake, *serviceConnectorsFake, *serviceStoreFake)
		want   string
	}{
		{"identity changed", func(s *Service, src stream.Source, _ *serviceSourceFake, _ *serviceTopicsFake, _ *serviceConnectorsFake, st *serviceStoreFake) {
			st.registered = registeredFor(s, src, [16]byte{1, 2, 3})
			st.registered.Identity.Table = "other"
		}, "established identity differs"},
		{"names changed", func(s *Service, src stream.Source, _ *serviceSourceFake, _ *serviceTopicsFake, _ *serviceConnectorsFake, st *serviceStoreFake) {
			st.registered = registeredFor(s, src, [16]byte{1, 2, 3})
			st.registered.Names.Connector = "other"
		}, "established capture names differ"},
		{"topic missing", func(s *Service, src stream.Source, _ *serviceSourceFake, tp *serviceTopicsFake, _ *serviceConnectorsFake, st *serviceStoreFake) {
			st.registered = registeredFor(s, src, [16]byte{1, 2, 3})
			tp.topicFacts.Exists = false
		}, "established topic is missing"},
		{"topic UUID changed", func(s *Service, src stream.Source, tp *serviceSourceFake, topics *serviceTopicsFake, _ *serviceConnectorsFake, st *serviceStoreFake) {
			_ = tp
			st.registered = registeredFor(s, src, [16]byte{9})
			topics.topicFacts.ID = [16]byte{1, 2, 3}
		}, "established topic has a different UUID"},
		{"partition count changed", func(s *Service, src stream.Source, _ *serviceSourceFake, topics *serviceTopicsFake, _ *serviceConnectorsFake, st *serviceStoreFake) {
			st.registered = registeredFor(s, src, [16]byte{1, 2, 3})
			topics.topicFacts.Partitions = 4
		}, "established partition count changed"},
		{"slot missing", func(s *Service, src stream.Source, sf *serviceSourceFake, _ *serviceTopicsFake, _ *serviceConnectorsFake, st *serviceStoreFake) {
			st.registered = registeredFor(s, src, [16]byte{1, 2, 3})
			sf.slot.Exists = false
		}, "established slot is missing"},
		{"WAL lost", func(s *Service, src stream.Source, sf *serviceSourceFake, _ *serviceTopicsFake, _ *serviceConnectorsFake, st *serviceStoreFake) {
			st.registered = registeredFor(s, src, [16]byte{1, 2, 3})
			sf.slot.WALStatus = WALLost
		}, "established slot has lost WAL history"},
		{"connector missing", func(s *Service, src stream.Source, _ *serviceSourceFake, _ *serviceTopicsFake, cf *serviceConnectorsFake, st *serviceStoreFake) {
			st.registered = registeredFor(s, src, [16]byte{1, 2, 3})
			cf.statusErr = ErrConnectorMissing
		}, "established connector is missing"},
	}
	for _, tc := range contractCases {
		t.Run(tc.name, func(t *testing.T) {
			service, source, sf, topics, connectors, store := newProvisionService(t)
			tc.mutate(service, source, sf, topics, connectors, store)
			store.found = true
			_, err := service.ProvisionSource(context.Background(), source)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want substring %q", err, tc.want)
			}
			if len(store.blockCalls) != 1 || !strings.Contains(store.blockCalls[0], tc.want) {
				t.Fatalf("block calls = %v, want %q", store.blockCalls, tc.want)
			}
		})
	}

	service, source, _, _, _, store := newProvisionService(t)
	store.found = true
	store.registered = registeredFor(service, source, [16]byte{1, 2, 3})
	got, err := service.ProvisionSource(context.Background(), source)
	if err != nil || !reflect.DeepEqual(got, store.registered) || len(store.blockCalls) != 0 {
		t.Fatalf("valid registration = %+v err=%v blocks=%v", got, err, store.blockCalls)
	}
}

func TestProvisionSourceRegisteredTransientFailuresDoNotBlock(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*serviceSourceFake, *serviceTopicsFake, *serviceConnectorsFake, *serviceStoreFake)
		want   string
	}{
		{"topic metadata", func(_ *serviceSourceFake, tp *serviceTopicsFake, _ *serviceConnectorsFake, _ *serviceStoreFake) {
			tp.topicErr = errors.New("kafka down")
		}, "retry provisioning"},
		{"slot health", func(sf *serviceSourceFake, _ *serviceTopicsFake, _ *serviceConnectorsFake, _ *serviceStoreFake) {
			sf.slotErr = errors.New("postgres down")
		}, "retry provisioning"},
		{"replication changed", func(_ *serviceSourceFake, tp *serviceTopicsFake, _ *serviceConnectorsFake, _ *serviceStoreFake) {
			tp.topicFacts.Replicas[0] = 1
		}, "retry provisioning"},
		{"connector failed", func(_ *serviceSourceFake, _ *serviceTopicsFake, cf *serviceConnectorsFake, _ *serviceStoreFake) {
			cf.status = Status{Connector: StateFailed, Failed: true}
		}, "retry provisioning"},
		{"connector error", func(_ *serviceSourceFake, _ *serviceTopicsFake, cf *serviceConnectorsFake, _ *serviceStoreFake) {
			cf.statusErr = errors.New("connect down")
		}, "retry provisioning"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			service, source, sf, topics, connectors, store := newProvisionService(t)
			store.found = true
			store.registered = registeredFor(service, source, [16]byte{1, 2, 3})
			tc.mutate(sf, topics, connectors, store)
			_, err := service.ProvisionSource(context.Background(), source)
			if err == nil || !strings.Contains(err.Error(), tc.want) || len(store.blockCalls) != 0 {
				t.Fatalf("error=%v blocks=%v", err, store.blockCalls)
			}
		})
	}
}

func TestProvisionSourceStoreAndInfrastructureErrors(t *testing.T) {
	t.Run("table inspection", func(t *testing.T) {
		service, source, sf, _, _, store := newProvisionService(t)
		sf.inspectErr = errors.New("database unavailable")
		_, err := service.ProvisionSource(context.Background(), source)
		if err == nil || err.Error() != "database unavailable" || len(store.blockCalls) != 0 {
			t.Fatalf("error=%v blocks=%v", err, store.blockCalls)
		}
	})
	t.Run("contract table", func(t *testing.T) {
		service, source, sf, _, _, store := newProvisionService(t)
		sf.facts.Exists = false
		_, err := service.ProvisionSource(context.Background(), source)
		if err == nil || !strings.Contains(err.Error(), "does not exist") || len(store.blockCalls) != 0 {
			t.Fatalf("error=%v blocks=%v", err, store.blockCalls)
		}
	})
	t.Run("store get", func(t *testing.T) {
		service, source, _, _, _, store := newProvisionService(t)
		store.getErr = errors.New("control unavailable")
		_, err := service.ProvisionSource(context.Background(), source)
		if err == nil || err.Error() != "control unavailable" || len(store.blockCalls) != 0 {
			t.Fatalf("error=%v blocks=%v", err, store.blockCalls)
		}
	})
	t.Run("topic ensure", func(t *testing.T) {
		service, source, _, topics, _, store := newProvisionService(t)
		topics.ensureErr = errors.New("topic create failed")
		_, err := service.ProvisionSource(context.Background(), source)
		if err == nil || err.Error() != "topic create failed" || len(store.blockCalls) != 0 {
			t.Fatalf("error=%v blocks=%v", err, store.blockCalls)
		}
	})
	t.Run("connector ensure", func(t *testing.T) {
		service, source, _, _, connectors, store := newProvisionService(t)
		connectors.ensureErr = errors.New("connector create failed")
		_, err := service.ProvisionSource(context.Background(), source)
		if err == nil || err.Error() != "connector create failed" || len(store.blockCalls) != 0 {
			t.Fatalf("error=%v blocks=%v", err, store.blockCalls)
		}
	})
	t.Run("register", func(t *testing.T) {
		service, source, _, _, _, store := newProvisionService(t)
		store.registerErr = errors.New("control write failed")
		_, err := service.ProvisionSource(context.Background(), source)
		if err == nil || err.Error() != "control write failed" || len(store.blockCalls) != 0 {
			t.Fatalf("error=%v blocks=%v", err, store.blockCalls)
		}
	})
	t.Run("blocked source", func(t *testing.T) {
		service, source, _, _, _, store := newProvisionService(t)
		store.found = true
		store.registered = registeredFor(service, source, [16]byte{1, 2, 3})
		store.registered.Blocked = "history lost"
		_, err := service.ProvisionSource(context.Background(), source)
		if err == nil || err.Error() != "control: source is blocked: history lost" || len(store.blockCalls) != 0 {
			t.Fatalf("error=%v blocks=%v", err, store.blockCalls)
		}
	})
	t.Run("block failure", func(t *testing.T) {
		service, source, _, topics, _, store := newProvisionService(t)
		store.found = true
		store.registered = registeredFor(service, source, [16]byte{1, 2, 3})
		topics.topicFacts.Exists = false
		store.blockErr = errors.New("control write failed")
		_, err := service.ProvisionSource(context.Background(), source)
		if err == nil || !strings.Contains(err.Error(), "block source: control write failed") || len(store.blockCalls) != 1 {
			t.Fatalf("error=%v blocks=%v", err, store.blockCalls)
		}
	})
}

func TestProvisionSourceContextCancellationStopsReadinessPoll(t *testing.T) {
	service, source, _, _, connectors, store := newProvisionService(t)
	connectors.status = Status{Connector: StateRestarting, Tasks: []ConnectorState{StateUnassigned}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	_, err := service.ProvisionSource(ctx, source)
	if err == nil || err.Error() != "capture did not become ready before provisioning timed out" || time.Since(started) > time.Second || store.registerCalls != 0 {
		t.Fatalf("canceled provisioning error=%v duration=%s register=%d", err, time.Since(started), store.registerCalls)
	}
}

func TestProvisionSourceRequiresRunningTasksAndRetainedWAL(t *testing.T) {
	for _, tc := range []struct {
		name  string
		tasks []ConnectorState
		wal   WALStatus
	}{
		{"paused task", []ConnectorState{StateRunning, StatePaused}, WALReserved},
		{"unassigned task", []ConnectorState{StateUnassigned}, WALReserved},
		{"lost WAL", []ConnectorState{StateRunning}, WALLost},
		{"unreserved WAL", []ConnectorState{StateRunning}, WALUnreserved},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, source, catalog, _, connectors, store := newProvisionService(t)
			connectors.status.Tasks = tc.tasks
			catalog.slot.WALStatus = tc.wal
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := service.ProvisionSource(ctx, source); err == nil || store.registerCalls != 0 {
				t.Fatalf("unready capture was registered: err=%v registrations=%d", err, store.registerCalls)
			}
		})
	}
}

func TestInspectSourceHealthOutcomes(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(*serviceSourceFake, *serviceTopicsFake, *serviceConnectorsFake, *stream.Registration)
		wantState  string
		wantReason string
	}{
		{"table unavailable", func(sf *serviceSourceFake, _ *serviceTopicsFake, _ *serviceConnectorsFake, _ *stream.Registration) {
			sf.inspectErr = errors.New("db down")
		}, "unavailable", "source database unavailable"},
		{"table contract", func(sf *serviceSourceFake, _ *serviceTopicsFake, _ *serviceConnectorsFake, _ *stream.Registration) {
			sf.facts.Exists = false
		}, "blocked", "source table violates capture contract"},
		{"identity changed", func(_ *serviceSourceFake, _ *serviceTopicsFake, _ *serviceConnectorsFake, r *stream.Registration) {
			r.Identity.Table = "other"
		}, "blocked", "stream identity changed"},
		{"names changed", func(_ *serviceSourceFake, _ *serviceTopicsFake, _ *serviceConnectorsFake, r *stream.Registration) {
			r.Names.Connector = "other"
		}, "blocked", "stream configuration changed"},
		{"topic unavailable", func(_ *serviceSourceFake, tp *serviceTopicsFake, _ *serviceConnectorsFake, _ *stream.Registration) {
			tp.topicErr = errors.New("kafka down")
		}, "unavailable", "Kafka metadata unavailable"},
		{"topic missing", func(_ *serviceSourceFake, tp *serviceTopicsFake, _ *serviceConnectorsFake, _ *stream.Registration) {
			tp.topicFacts.Exists = false
		}, "blocked", "established topic missing"},
		{"replication mismatch", func(_ *serviceSourceFake, tp *serviceTopicsFake, _ *serviceConnectorsFake, _ *stream.Registration) {
			tp.topicFacts.Replicas[0] = 1
		}, "unavailable", "capture: topic \"postie_g1_a7860cddf9b4f43a35a290871336.public.event_store\" partition 0 has 1 replicas, want 2"},
		{"topic identity changed", func(_ *serviceSourceFake, tp *serviceTopicsFake, _ *serviceConnectorsFake, _ *stream.Registration) {
			tp.topicFacts.ID = [16]byte{9}
		}, "blocked", "topic identity changed"},
		{"slot unavailable", func(sf *serviceSourceFake, _ *serviceTopicsFake, _ *serviceConnectorsFake, _ *stream.Registration) {
			sf.slotErr = errors.New("postgres down")
		}, "unavailable", "replication slot health unavailable"},
		{"slot missing", func(sf *serviceSourceFake, _ *serviceTopicsFake, _ *serviceConnectorsFake, _ *stream.Registration) {
			sf.slot.Exists = false
		}, "blocked", "replication slot or WAL history lost"},
		{"WAL lost", func(sf *serviceSourceFake, _ *serviceTopicsFake, _ *serviceConnectorsFake, _ *stream.Registration) {
			sf.slot.WALStatus = WALLost
		}, "blocked", "replication slot or WAL history lost"},
		{"connector missing", func(_ *serviceSourceFake, _ *serviceTopicsFake, cf *serviceConnectorsFake, _ *stream.Registration) {
			cf.statusErr = ErrConnectorMissing
		}, "blocked", "established connector missing"},
		{"connector failed", func(_ *serviceSourceFake, _ *serviceTopicsFake, cf *serviceConnectorsFake, _ *stream.Registration) {
			cf.status = Status{Connector: StateFailed, Failed: true}
		}, "unavailable", "capture connector unavailable"},
		{"connector paused", func(_ *serviceSourceFake, _ *serviceTopicsFake, cf *serviceConnectorsFake, _ *stream.Registration) {
			cf.status = Status{Connector: StatePaused, Tasks: []ConnectorState{StateRunning}}
		}, "unavailable", "capture connector unavailable"},
		{"no tasks", func(_ *serviceSourceFake, _ *serviceTopicsFake, cf *serviceConnectorsFake, _ *stream.Registration) {
			cf.status = Status{Connector: StateRunning}
		}, "unavailable", "capture connector unavailable"},
		{"task unavailable", func(_ *serviceSourceFake, _ *serviceTopicsFake, cf *serviceConnectorsFake, _ *stream.Registration) {
			cf.status = Status{Connector: StateRunning, Tasks: []ConnectorState{StateRunning, StateUnassigned}}
		}, "unavailable", "capture task unavailable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			service, source, sf, topics, connectors, _ := newProvisionService(t)
			registered := registeredFor(service, source, topics.topicID)
			tc.mutate(sf, topics, connectors, &registered)
			state, reason := service.InspectSource(context.Background(), source, registered)
			if state != tc.wantState || reason != tc.wantReason {
				t.Fatalf("InspectSource = %q, %q; want %q, %q", state, reason, tc.wantState, tc.wantReason)
			}
		})
	}
	service, source, sf, topics, connectors, _ := newProvisionService(t)
	registered := registeredFor(service, source, topics.topicID)
	state, reason := service.InspectSource(context.Background(), source, registered)
	if state != "running" || reason != "" || sf.inspectCall != 1 || len(connectors.statusCalls) != 1 {
		t.Fatalf("healthy InspectSource = %q, %q inspect=%d status=%d", state, reason, sf.inspectCall, len(connectors.statusCalls))
	}
}

func TestInspectSourceUsesExternalPublication(t *testing.T) {
	service, source, _, topics, _, _ := newProvisionService(t)
	service.Publications[source.ID] = "external_pub"
	registered := registeredFor(service, source, topics.topicID)
	registered.Names.Publication = "external_pub"
	state, reason := service.InspectSource(context.Background(), source, registered)
	if state != "running" || reason != "" {
		t.Fatalf("InspectSource external publication = %q, %q", state, reason)
	}
}

func TestProvisionSourceRegisteredContractBlockFailureIsRetryable(t *testing.T) {
	service, source, _, topics, _, store := newProvisionService(t)
	store.found = true
	store.registered = registeredFor(service, source, topics.topicID)
	topics.topicFacts.Exists = false
	store.blockErr = errors.New("store unavailable")
	_, err := service.ProvisionSource(context.Background(), source)
	if err == nil || !strings.Contains(err.Error(), "capture: table public.event_store: established topic is missing") || !strings.Contains(err.Error(), "block source: store unavailable") {
		t.Fatalf("block failure error = %v", err)
	}
}

func TestValidateReplicationSortsPartitionsInError(t *testing.T) {
	err := validateReplication("events", TopicFacts{Replicas: map[int32]int{4: 1, 1: 3}}, 2)
	if err == nil || !strings.Contains(fmt.Sprint(err), "partition 1") {
		t.Fatalf("validateReplication error = %v", err)
	}
}

func TestTopicMetadataDiagnosticsRemainDistinct(t *testing.T) {
	for _, tc := range []struct {
		name              string
		err               error
		status, provision string
	}{
		{"broker", errors.New("broker disconnected"), "Kafka metadata unavailable", "topic inspection unavailable"},
		{"topic", &TopicMetadataError{Cause: errors.New("topic unauthorized")}, "Kafka topic unavailable", "topic inspection unavailable"},
		{"partition", &TopicMetadataError{Partition: true, Cause: errors.New("capture: inspect topic events partition 2: leader unavailable")}, "capture: inspect topic events partition 2: leader unavailable", "capture: inspect topic events partition 2: leader unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, source, _, topics, _, store := newProvisionService(t)
			topics.topicErr = tc.err
			store.registered = registeredFor(service, source, topics.topicID)
			store.found = true
			state, reason := service.InspectSource(context.Background(), source, store.registered)
			if state != "unavailable" || reason != tc.status {
				t.Fatalf("status = %q %q; want unavailable %q", state, reason, tc.status)
			}
			_, err := service.ProvisionSource(context.Background(), source)
			if err == nil || err.Error() != "capture health could not be verified; retry provisioning: "+tc.provision || len(store.blockCalls) != 0 {
				t.Fatalf("provision error=%v blocks=%v", err, store.blockCalls)
			}
		})
	}
}

var validIdentifier = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)

func testIdentity(src stream.Source) stream.Identity {
	columns := make([]stream.Column, len(src.Columns))
	for i, name := range src.Columns {
		columns[i] = stream.Column{Name: name, Type: stream.PGText}
	}
	return stream.Identity{Table: src.Table, SerialColumn: src.SerialColumn, PartitioningColumn: src.PartitioningColumn, Columns: columns}
}

func TestNamesForIsDeterministicAndValid(t *testing.T) {
	cases := []struct {
		name        string
		namespace   string
		environment string
		src         stream.Source
		generation  stream.Generation
	}{
		{"simple", "postie", "production", stream.Source{ID: "orders", Table: "event_store"}, 1},
		{"max-generation", "postie", "production", stream.Source{ID: "orders", Table: "event_store"}, stream.Generation(int(^uint(0) >> 1))},
		{"dots-in-ids", "multi.tenant", "prod.us-east", stream.Source{ID: "src.one", Table: "event_store"}, 2},
		{"uppercase", "Postie", "Production", stream.Source{ID: "Orders-ID", Table: "Event_Store"}, 3},
		{"very-long", "namespace-that-is-quite-long-indeed", "environment-also-rather-long", stream.Source{ID: "a-source-identifier-that-goes-on-and-on-and-on-for-a-while", Table: "event_store"}, 7},
		{"very-long-2", "namespace-that-is-quite-long-indeed", "environment-also-rather-long", stream.Source{ID: "a-source-identifier-that-goes-on-and-on-and-on-for-a-while-too", Table: "event_store"}, 7},
	}
	results := map[string]stream.Names{}
	for _, c := range cases {
		n := NamesFor(c.namespace, c.environment, c.src, c.generation)
		results[c.name] = n
		if !validIdentifier.MatchString(n.Slot) || !validIdentifier.MatchString(n.Publication) {
			t.Errorf("%s: resource name is invalid: %+v", c.name, n)
		}
		if !strings.HasPrefix(n.Slot, "postie_") || !strings.HasPrefix(n.Publication, "postie_") {
			t.Errorf("%s: resource names do not have postie_ prefix: %+v", c.name, n)
		}
		if again := NamesFor(c.namespace, c.environment, c.src, c.generation); again != n {
			t.Errorf("%s: NamesFor is not deterministic: %+v != %+v", c.name, again, n)
		}
		if n.Topic != n.TopicPrefix+".public."+c.src.Table {
			t.Errorf("%s: Topic = %q", c.name, n.Topic)
		}
		if strings.Contains(n.Connector, ".") {
			t.Errorf("%s: Connector %q still contains a dot", c.name, n.Connector)
		}
	}
	if results["very-long"].Slot == results["very-long-2"].Slot || results["very-long"].Publication == results["very-long-2"].Publication {
		t.Fatal("two different long source ids produced the same resource name")
	}
}

func TestIdentityFromRejections(t *testing.T) {
	src := stream.Source{ID: "orders", Table: "event_store", Columns: []string{"id", "correlation_id", "payload", "event_id"}, SerialColumn: "id", PartitioningColumn: "correlation_id"}
	validFacts := func() TableFacts {
		return TableFacts{Exists: true, Columns: map[string]ColumnFacts{
			"id": {Type: stream.PGInt8, NotNull: true}, "correlation_id": {Type: stream.PGText, NotNull: true},
			"payload": {Type: stream.PGText, NotNull: true}, "event_id": {Type: stream.PGText, NotNull: true},
		}, SerialUniqueIndex: true}
	}
	cases := []struct {
		name  string
		facts func() TableFacts
	}{
		{"missing-table", func() TableFacts { f := validFacts(); f.Exists = false; return f }},
		{"missing-column", func() TableFacts { f := validFacts(); delete(f.Columns, "payload"); return f }},
		{"nullable-serial", func() TableFacts {
			f := validFacts()
			f.Columns["id"] = ColumnFacts{Type: stream.PGInt8}
			return f
		}},
		{"text-serial", func() TableFacts {
			f := validFacts()
			f.Columns["id"] = ColumnFacts{Type: stream.PGText, NotNull: true}
			return f
		}},
		{"no-unique-index-on-serial", func() TableFacts { f := validFacts(); f.SerialUniqueIndex = false; return f }},
		{"nullable-partitioning", func() TableFacts {
			f := validFacts()
			f.Columns["correlation_id"] = ColumnFacts{Type: stream.PGText}
			return f
		}},
		{"jsonb", func() TableFacts {
			f := validFacts()
			f.Columns["payload"] = ColumnFacts{Type: stream.PGType("jsonb"), NotNull: true}
			return f
		}},
		{"uuid", func() TableFacts {
			f := validFacts()
			f.Columns["payload"] = ColumnFacts{Type: stream.PGType("uuid"), NotNull: true}
			return f
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := IdentityFrom(src, c.facts(), 5)
			if err == nil {
				t.Fatal("identityFrom succeeded, want an error")
			}
			var contractErr *ContractError
			if !errors.As(err, &contractErr) {
				t.Fatalf("identityFrom error = %v (%T), want *ContractError", err, err)
			}
			if contractErr.Table != src.Table {
				t.Errorf("ContractError.Table = %q, want %q", contractErr.Table, src.Table)
			}
		})
	}
	got, err := IdentityFrom(src, validFacts(), 5)
	if err != nil {
		t.Fatal(err)
	}
	want := stream.Identity{Table: "event_store", SerialColumn: "id", PartitioningColumn: "correlation_id", EventIDColumn: "event_id", Partitions: 5, Columns: []stream.Column{{Name: "id", Type: stream.PGInt8}, {Name: "correlation_id", Type: stream.PGText}, {Name: "payload", Type: stream.PGText}, {Name: "event_id", Type: stream.PGText}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("identityFrom = %+v, want %+v", got, want)
	}
}

func TestIdentityFromAcceptsNullableJSON(t *testing.T) {
	src := testSource()
	facts := TableFacts{Exists: true, Columns: map[string]ColumnFacts{
		"id":             {Type: stream.PGInt8, NotNull: true},
		"correlation_id": {Type: stream.PGText, NotNull: true},
		"payload":        {Type: stream.PGJSON},
	}, SerialUniqueIndex: true}

	got, err := IdentityFrom(src, facts, 3)
	if err != nil {
		t.Fatalf("IdentityFrom() error = %v", err)
	}
	want := testIdentity(src)
	want.Partitions = 3
	want.Columns[0].Type = stream.PGInt8
	want.Columns[2].Type = stream.PGJSON
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("IdentityFrom() = %+v, want %+v", got, want)
	}
}

func TestRegression_CaptureNamesPreserveIdentity(t *testing.T) {
	type tuple struct {
		namespace   string
		environment string
		sourceID    string
	}
	tuples := []tuple{
		{"ns", "dev", "orders.v1"},
		{"ns", "dev", "orders-v1"},
		{"ns", "dev", "orders_v1"},
		{"ns", "dev", "Orders_v1"},
		{"a.b", "c", "orders"},
		{"a", "b.c", "orders"},
	}
	fields := func(n stream.Names) []string {
		return []string{n.TopicPrefix, n.Topic, n.Connector, n.Slot, n.Publication}
	}

	all := make([][]string, len(tuples))
	for i, tc := range tuples {
		n := NamesFor(tc.namespace, tc.environment, stream.Source{ID: tc.sourceID, Table: "events"}, 1)
		all[i] = fields(n)
		for j, got := range all[i] {
			if got == "" {
				t.Errorf("tuple %d resource field %d is empty", i, j)
			}
		}
	}
	for i := range all {
		for j := i + 1; j < len(all); j++ {
			for k := range fields(NamesFor("ns", "dev", stream.Source{ID: "orders", Table: "events"}, 1)) {
				if all[i][k] == all[j][k] {
					t.Errorf("resource field %d collides for tuples %d and %d: %q", k, i, j, all[i][k])
				}
			}
		}
	}

	longBase := strings.Repeat("source-", 14)
	longA := NamesFor("a-very-long-namespace", "a-very-long-environment", stream.Source{ID: longBase + "alpha", Table: "events"}, 1)
	longB := NamesFor("a-very-long-namespace", "a-very-long-environment", stream.Source{ID: longBase + "bravo", Table: "events"}, 1)
	if longA.Slot == longB.Slot || longA.Publication == longB.Publication {
		t.Fatalf("long source IDs differing only in their suffix collided: A=%+v B=%+v", longA, longB)
	}
	if again := NamesFor("a-very-long-namespace", "a-very-long-environment", stream.Source{ID: longBase + "alpha", Table: "events"}, 1); again != longA {
		t.Fatalf("NamesFor is not deterministic: first=%+v again=%+v", longA, again)
	}
	generationOne := fields(NamesFor("ns", "dev", stream.Source{ID: "orders", Table: "events"}, 1))
	generationTwo := fields(NamesFor("ns", "dev", stream.Source{ID: "orders", Table: "events"}, 2))
	for i := range generationOne {
		if generationOne[i] == generationTwo[i] {
			t.Errorf("resource field %d did not change between generations: %q", i, generationOne[i])
		}
	}

	maxGeneration := stream.Generation(int(^uint(0) >> 1))
	maxNames := NamesFor(strings.Repeat("n", 24), strings.Repeat("e", 24), stream.Source{ID: strings.Repeat("s", 24), Table: "events"}, maxGeneration)
	for label, value := range map[string]string{"slot": maxNames.Slot, "publication": maxNames.Publication} {
		if len(value) > 63 {
			t.Errorf("%s name length = %d, want <= 63: %q", label, len(value), value)
		}
		for _, r := range value {
			if !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9') && r != '_' {
				t.Errorf("%s name contains invalid PostgreSQL identifier rune %U: %q", label, r, value)
			}
		}
	}
}
