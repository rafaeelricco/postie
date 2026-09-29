package app

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rafaeelricco/postie/internal/adapters/controlpg"
	"github.com/rafaeelricco/postie/internal/adapters/debezium"
	"github.com/rafaeelricco/postie/internal/adapters/kafka"
	"github.com/rafaeelricco/postie/internal/adapters/sourcepg"
	"github.com/rafaeelricco/postie/internal/config"
	"github.com/rafaeelricco/postie/internal/provision"
	"github.com/rafaeelricco/postie/internal/stream"
)

// Resources owns the adapters shared by provisioning and a running engine.
type Resources struct {
	provision *provision.Service
	store     *controlpg.Store
	kafka     *kafka.Admin
	sources   map[string]stream.Source
	pools     []*sourcepg.Client
	scope     stream.Scope
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
// must Close; the per-source pools are created here but connect lazily, on
// the first inspection.
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
	resources := &Resources{store: store, kafka: admin, scope: scope, sources: map[string]stream.Source{}}
	inspectors := map[string]provision.Source{}
	connections := map[string]debezium.Connection{}
	publications := map[string]string{}
	for _, input := range application.Sources {
		source, connection := Source(input), connectionOf(input)
		resources.sources[source.ID] = source
		client := sourcepg.Open(source, connection)
		resources.pools = append(resources.pools, client)
		inspectors[source.ID] = client
		connections[source.ID] = debezium.Connection(connection) // one set of fields, so the two cannot drift
		publications[source.ID] = engine.Sources[source.ID].Publication
	}
	resources.provision = &provision.Service{
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

// Close releases the Kafka admin client, the control store, and every
// source pool.
func (r *Resources) Close() {
	r.kafka.Close()
	r.store.Close()
	for _, pool := range r.pools {
		pool.Close()
	}
}

// Source maps the configuration boundary into the credential-free stream model.
func Source(input config.Source) stream.Source {
	return stream.Source{ID: input.ID, Description: input.Description, Table: input.Table, Columns: append([]string(nil), input.Columns...), SerialColumn: input.SerialColumn, PartitioningColumn: input.PartitioningColumn}
}

const (
	bootstrapTimeout = 5 * time.Second  // opening the adapters
	perSourceTimeout = 60 * time.Second // one source's provisioning
)

// Provision establishes capture for every configured source, then records the
// subscriptions. It returns the process exit code: 0 all provisioned, 1 any failure.
func Provision(engine config.Engine, application config.Application, generation stream.Generation, stdout, stderr io.Writer) int {
	ctx, cancel := context.WithTimeout(context.Background(), bootstrapTimeout)
	resources, err := Bootstrap(ctx, engine, application, generation)
	cancel()
	if err != nil {
		reportBootstrapFailure(stderr, application.Sources, err)
		return 1
	}
	defer resources.Close()
	if !provisionSources(resources, application.Sources, generation, stdout, stderr) {
		return 1
	}
	return ensureSubscriptions(resources, destinationIDs(application.Destinations), stderr)
}

// reportBootstrapFailure prints err once when the Kafka client could not be
// created, and once per source id for any other failure.
func reportBootstrapFailure(stderr io.Writer, sources []config.Source, err error) {
	if initialization, ok := err.(*InitializationError); ok && initialization.Dependency == DependencyKafka {
		fmt.Fprintln(stderr, err)
		return
	}
	for _, source := range sources {
		fmt.Fprintf(stderr, "%s: %v\n", source.ID, err)
	}
}

// provisionSources establishes capture for every source, trying each one
// even after an earlier failure, and reports whether all of them succeeded.
func provisionSources(resources *Resources, sources []config.Source, generation stream.Generation, stdout, stderr io.Writer) bool {
	ok := true
	for _, source := range sources {
		ctx, cancel := context.WithTimeout(context.Background(), perSourceTimeout)
		registered, err := resources.provision.ProvisionSource(ctx, resources.sources[source.ID])
		cancel()
		if err != nil {
			fmt.Fprintf(stderr, "%s: %v\n", source.ID, err)
			ok = false
			continue
		}
		names := registered.Names
		fmt.Fprintf(stdout, "provisioned source=%s generation=%s topic=%s connector=%s slot=%s publication=%s\n", source.ID, generation, names.Topic, names.Connector, names.Slot, names.Publication)
	}
	return ok
}

// destinationIDs returns the id of every destination, in order.
func destinationIDs(destinations []config.Destination) []string {
	ids := make([]string, len(destinations))
	for i, destination := range destinations {
		ids[i] = destination.ID
	}
	return ids
}

// ensureSubscriptions records the given subscriptions and returns the exit code.
func ensureSubscriptions(resources *Resources, ids []string, stderr io.Writer) int {
	if err := resources.store.EnsureSubscriptions(context.Background(), resources.scope, ids); err != nil {
		fmt.Fprintf(stderr, "subscriptions: %v\n", err)
		return 1
	}
	return 0
}
