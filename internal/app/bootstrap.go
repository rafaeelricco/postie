// Package app composes engine policies with their external-system adapters.
package app

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/adapters/controlpg"
	"github.com/rafaeelricco/postie/internal/adapters/debezium"
	"github.com/rafaeelricco/postie/internal/adapters/httpdelivery"
	"github.com/rafaeelricco/postie/internal/adapters/kafka"
	"github.com/rafaeelricco/postie/internal/adapters/sourcepg"
	"github.com/rafaeelricco/postie/internal/config"
	"github.com/rafaeelricco/postie/internal/control"
	"github.com/rafaeelricco/postie/internal/delivery"
	"github.com/rafaeelricco/postie/internal/protocol"
	"github.com/rafaeelricco/postie/internal/provision"
	"github.com/rafaeelricco/postie/internal/stream"
)

// Resources owns the adapters shared by provisioning and a running engine.
type Resources struct {
	Provision *provision.Service
	Store     *controlpg.Store
	Kafka     *kafka.Admin
	Sources   map[string]stream.Source
	Scope     stream.Scope
}

// Dependency names a startup dependency that can fail.
type Dependency string

const (
	DependencyKafka   Dependency = "Kafka"
	DependencyControl Dependency = "control"
)

// InitializationError preserves the command-specific startup diagnostics while
// keeping adapter construction in the composition layer.
type InitializationError struct {
	Dependency Dependency
	Err        error
}

// Error returns the wrapped error's message.
func (e *InitializationError) Error() string { return e.Err.Error() }

// Bootstrap builds the adapters that provisioning and a running engine share.
// It opens the Kafka admin client and the control store, which the caller
// must Close; the per-source inspectors connect only when they are called.
func Bootstrap(ctx context.Context, engine config.Engine, application config.Application, generation stream.Generation) (*Resources, error) {
	if !generation.Valid() {
		return nil, fmt.Errorf("generation must be positive")
	}
	admin, err := kafka.NewAdmin(engine.Kafka.Brokers)
	if err != nil {
		return nil, &InitializationError{Dependency: DependencyKafka, Err: err}
	}
	store, err := controlpg.Open(ctx, engine.Control.DatabaseURL)
	if err != nil {
		admin.Close()
		return nil, &InitializationError{Dependency: DependencyControl, Err: err}
	}
	scope := stream.Scope{Namespace: engine.Namespace, Environment: engine.Environment, Generation: generation}
	resources := &Resources{Store: store, Kafka: admin, Scope: scope, Sources: map[string]stream.Source{}}
	inspectors := map[string]provision.Source{}
	connections := map[string]debezium.Connection{}
	publications := map[string]string{}
	for _, input := range application.Sources {
		source, connection := Source(input), connectionOf(input)
		resources.Sources[source.ID] = source
		inspectors[source.ID] = &sourcepg.Client{Source: source, Connection: connection}
		connections[source.ID] = debezium.Connection(connection) // one set of fields, so the two cannot drift
		publications[source.ID] = engine.Sources[source.ID].Publication
	}
	resources.Provision = &provision.Service{
		Scope: scope, Partitions: engine.Kafka.Partitions, Replication: engine.Kafka.ReplicationFactor,
		Publications: publications, Sources: inspectors, Topics: admin, Store: store,
		Connectors: &debezium.Client{URL: engine.Connect.URL, HTTP: &http.Client{Timeout: 30 * time.Second}, Connections: connections},
	}
	return resources, nil
}

// connectionOf picks the database connection settings out of a source.
func connectionOf(input config.Source) sourcepg.Connection {
	return sourcepg.Connection{Host: input.Host, Port: input.Port, Username: input.Username, Password: input.Password, Database: input.Database}
}

// Close releases the Kafka admin client and control store connections.
func (r *Resources) Close() { r.Kafka.Close(); r.Store.Close() }

// Source maps the configuration boundary into the credential-free stream model.
func Source(input config.Source) stream.Source {
	return stream.Source{ID: input.ID, Description: input.Description, Table: input.Table, Columns: append([]string(nil), input.Columns...), SerialColumn: input.SerialColumn, PartitioningColumn: input.PartitioningColumn}
}

// App owns a running control service and its adapter lifetimes.
type App struct {
	*control.Service
	resources *Resources
}

// New builds a running engine: shared adapters, then a control service whose
// consumers are assembled per destination by wiring.consumer. Startup errors
// are reduced to one fixed sentence, so no part of a connection string is
// ever returned.
func New(ctx context.Context, engine config.Engine, application config.Application, generation stream.Generation, writer io.Writer) (*App, error) {
	resources, err := Bootstrap(ctx, engine, application, generation)
	if err != nil {
		return nil, startupError(err)
	}
	w := wiring{
		engine:       engine,
		resources:    resources,
		destinations: destinationsByID(application.Destinations),
		generation:   generation,
		log:          activity.New(writer),
	}
	service, err := control.New(ctx, control.Options{
		Scope: resources.Scope, Sources: orderedSources(application.Sources, resources.Sources),
		Destinations: controlDestinations(application.Destinations),
		Store:        resources.Store, Monitor: resources.Provision, Consumers: w.consumer, Kafka: resources.Kafka, Log: w.log,
	})
	if err != nil {
		resources.Close()
		return nil, err
	}
	return &App{Service: service, resources: resources}, nil
}

// startupError replaces a Bootstrap dependency failure with one fixed
// sentence per dependency. Any other error passes through unchanged.
func startupError(err error) error {
	initialization, ok := err.(*InitializationError)
	if !ok {
		return err
	}
	if initialization.Dependency == DependencyKafka {
		return fmt.Errorf("Kafka client could not be created")
	}
	return fmt.Errorf("control store could not be opened")
}

// destinationsByID indexes destinations by ID for lookup while building each
// destination's consumer.
func destinationsByID(destinations []config.Destination) map[string]config.Destination {
	byID := make(map[string]config.Destination, len(destinations))
	for _, d := range destinations {
		byID[d.ID] = d
	}
	return byID
}

// controlDestinations narrows destination configuration to what control
// needs to track subscriptions: an ID and its source list.
func controlDestinations(destinations []config.Destination) []control.Destination {
	out := make([]control.Destination, 0, len(destinations))
	for _, d := range destinations {
		out = append(out, control.Destination{ID: d.ID, Sources: append([]string(nil), d.Sources...)})
	}
	return out
}

// orderedSources returns the bootstrapped sources in application config order.
func orderedSources(inputs []config.Source, byID map[string]stream.Source) []stream.Source {
	out := make([]stream.Source, 0, len(inputs))
	for _, source := range inputs {
		out = append(out, byID[source.ID])
	}
	return out
}

// filterFor converts an optional destination filter into the protocol type,
// copying its values so the destination config cannot alias the runtime one.
func filterFor(input *config.Filter) *protocol.Filter {
	if input == nil {
		return nil
	}
	return &protocol.Filter{Column: input.Column, Values: append([]string(nil), input.Values...)}
}

// wiring holds what the per-destination consumer and worker factories close
// over: the shared engine config and adapters, destination-keyed delivery
// settings, the stream generation, and the activity log.
type wiring struct {
	engine       config.Engine
	resources    *Resources
	destinations map[string]config.Destination
	generation   stream.Generation
	log          *activity.Log
}

// consumer builds the Kafka consumer for one destination: one delivery
// worker per source it subscribes to, keyed by that source's topic so
// incoming records can be routed to the right worker.
func (w wiring) consumer(ctx context.Context, destination control.Destination, registered map[string]stream.Registration, dispatch control.Dispatch) (control.Consumer, error) {
	input := w.destinations[destination.ID]
	topics := map[string]stream.Source{}
	workers := map[string]*delivery.Worker{}
	for _, id := range destination.Sources {
		source := w.resources.Sources[id]
		registration := registered[id]
		topics[registration.Names.Topic] = source
		workers[registration.Names.Topic] = w.worker(input, source, registration, dispatch)
	}
	return kafka.NewConsumer(ctx, kafka.ConsumerOptions{
		Brokers:     w.engine.Kafka.Brokers,
		Scope:       w.resources.Scope,
		Destination: destination.ID,
		Sources:     topics,
		Admin:       w.resources.Kafka,
		Store:       w.resources.Store,
		Dispatch:    dispatch,
		Log:         w.log,
		Process: func(ctx context.Context, raw *stream.RawRecord, commit kafka.CommitFunc) error {
			return workers[raw.Topic].Process(ctx, raw, commit)
		},
	})
}

// worker builds the delivery worker for one source feeding one destination:
// an HTTP client whose requests are fenced by the dispatch lease, a processor
// applying the destination's filter, and a decoder bound to the source's
// registered identity.
func (w wiring) worker(input config.Destination, source stream.Source, registration stream.Registration, dispatch control.Dispatch) *delivery.Worker {
	client := httpdelivery.NewClient(
		&http.Client{
			Timeout:   w.engine.Delivery.RequestTimeout,
			Transport: httpdelivery.LeaseTransport{Gate: dispatch, Source: source.ID},
		},
		httpdelivery.Destination{ID: input.ID, Endpoint: input.Endpoint, Username: input.Username, Password: input.Password},
	)
	destination := delivery.Destination{ID: input.ID, Description: input.Description, Filter: filterFor(input.Filter)}
	return &delivery.Worker{
		Processor: delivery.NewProcessor(destination, client),
		Decode: func(raw *stream.RawRecord) (stream.Record, error) {
			return debezium.Decode(source, registration.Identity, w.generation, raw)
		},
		Store:       w.resources.Store,
		Scope:       w.resources.Scope,
		Destination: input.ID,
		SourceID:    source.ID,
		Gate:        dispatch,
		Log:         w.log,
	}
}

// Close releases the resources this App's adapters hold.
func (a *App) Close() { a.resources.Close() }
