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

// InitializationError preserves the command-specific startup diagnostics while
// keeping adapter construction in the composition layer.
type InitializationError struct {
	Dependency string
	Err        error
}

func (e *InitializationError) Error() string { return e.Err.Error() }

func Bootstrap(ctx context.Context, engine config.Engine, application config.Application, generation stream.Generation) (*Resources, error) {
	if !generation.Valid() {
		return nil, fmt.Errorf("generation must be positive")
	}
	admin, err := kafka.NewAdmin(engine.Kafka.Brokers)
	if err != nil {
		return nil, &InitializationError{Dependency: "Kafka", Err: err}
	}
	store, err := controlpg.Open(ctx, engine.Control.DatabaseURL)
	if err != nil {
		admin.Close()
		return nil, &InitializationError{Dependency: "control", Err: err}
	}
	resources := &Resources{Store: store, Kafka: admin, Scope: stream.Scope{Namespace: engine.Namespace, Environment: engine.Environment, Generation: generation}, Sources: map[string]stream.Source{}}
	sources := map[string]provision.Source{}
	connections := map[string]debezium.Connection{}
	publications := map[string]string{}
	for _, input := range application.Sources {
		source := Source(input)
		resources.Sources[source.ID] = source
		sources[source.ID] = &sourcepg.Client{Source: source, Connection: sourcepg.Connection{Host: input.Host, Port: input.Port, Username: input.Username, Password: input.Password, Database: input.Database}}
		connections[source.ID] = debezium.Connection{Host: input.Host, Port: input.Port, Username: input.Username, Password: input.Password, Database: input.Database}
		publications[source.ID] = engine.Sources[source.ID].Publication
	}
	resources.Provision = &provision.Service{Scope: resources.Scope, Partitions: engine.Kafka.Partitions, Replication: engine.Kafka.ReplicationFactor, Publications: publications, Sources: sources, Topics: admin, Connectors: &debezium.Client{URL: engine.Connect.URL, HTTP: &http.Client{Timeout: 30 * time.Second}, Connections: connections}, Store: store}
	return resources, nil
}

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

func New(ctx context.Context, engine config.Engine, application config.Application, generation stream.Generation, writer io.Writer) (*App, error) {
	resources, err := Bootstrap(ctx, engine, application, generation)
	if err != nil {
		if initialization, ok := err.(*InitializationError); ok {
			if initialization.Dependency == "Kafka" {
				return nil, fmt.Errorf("Kafka client could not be created")
			}
			return nil, fmt.Errorf("control store could not be opened")
		}
		return nil, err
	}
	log := activity.New(writer)
	destinations := make([]control.Destination, 0, len(application.Destinations))
	deliveryConfigs := make(map[string]config.Destination, len(application.Destinations))
	for _, d := range application.Destinations {
		destinations = append(destinations, control.Destination{ID: d.ID, Sources: append([]string(nil), d.Sources...)})
		deliveryConfigs[d.ID] = d
	}
	sources := make([]stream.Source, 0, len(application.Sources))
	for _, source := range application.Sources {
		sources = append(sources, resources.Sources[source.ID])
	}
	factory := func(ctx context.Context, destination control.Destination, registered map[string]stream.Registration, dispatch control.Dispatch) (control.Consumer, error) {
		input := deliveryConfigs[destination.ID]
		topics := map[string]stream.Source{}
		workers := map[string]*delivery.Worker{}
		for _, id := range destination.Sources {
			source := resources.Sources[id]
			registration := registered[id]
			topics[registration.Names.Topic] = source
			var filter *protocol.Filter
			if input.Filter != nil {
				filter = &protocol.Filter{Column: input.Filter.Column, Values: append([]string(nil), input.Filter.Values...)}
			}
			client := httpdelivery.NewClient(&http.Client{Timeout: engine.Delivery.RequestTimeout, Transport: httpdelivery.LeaseTransport{Gate: dispatch, Source: source.ID}}, httpdelivery.Destination{ID: input.ID, Endpoint: input.Endpoint, Username: input.Username, Password: input.Password})
			processor := delivery.NewProcessor(delivery.Destination{ID: input.ID, Description: input.Description, Filter: filter}, client)
			workers[registration.Names.Topic] = &delivery.Worker{Processor: processor, Decode: func(raw *stream.RawRecord) (stream.Record, error) {
				return debezium.Decode(source, registration.Identity, generation, raw)
			}, Store: resources.Store, Scope: resources.Scope, Destination: destination.ID, SourceID: source.ID, Gate: dispatch, Log: log}
		}
		return kafka.NewConsumer(ctx, kafka.ConsumerOptions{Brokers: engine.Kafka.Brokers, Scope: resources.Scope, Destination: destination.ID, Sources: topics, Admin: resources.Kafka, Store: resources.Store, Dispatch: dispatch, Log: log, Process: func(ctx context.Context, raw *stream.RawRecord, commit func(context.Context, *stream.RawRecord) error) error {
			return workers[raw.Topic].Process(ctx, raw, commit)
		}})
	}
	service, err := control.New(ctx, control.Options{Scope: resources.Scope, Sources: sources, Destinations: destinations, Store: resources.Store, Monitor: resources.Provision, Consumers: factory, Kafka: resources.Kafka, Log: log})
	if err != nil {
		resources.Close()
		return nil, err
	}
	return &App{Service: service, resources: resources}, nil
}

func (a *App) Close() { a.resources.Close() }
