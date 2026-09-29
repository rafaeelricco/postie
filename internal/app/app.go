// Package app composes engine policies with their external-system adapters.
package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/adapters/debezium"
	"github.com/rafaeelricco/postie/internal/adapters/httpdelivery"
	"github.com/rafaeelricco/postie/internal/adapters/kafka"
	"github.com/rafaeelricco/postie/internal/adapters/operator"
	"github.com/rafaeelricco/postie/internal/config"
	"github.com/rafaeelricco/postie/internal/control"
	"github.com/rafaeelricco/postie/internal/delivery"
	"github.com/rafaeelricco/postie/internal/protocol"
	"github.com/rafaeelricco/postie/internal/stream"
)

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
		log:          activity.New(writer),
	}
	service, err := control.New(ctx, control.Options{
		Scope: resources.scope, Sources: orderedSources(application.Sources, resources.sources),
		Destinations: controlDestinations(application.Destinations),
		Store:        resources.store, Monitor: resources.provision, Consumers: w.consumer, Kafka: resources.kafka, Log: w.log,
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
// settings, and the activity log.
type wiring struct {
	engine       config.Engine
	resources    *Resources
	destinations map[string]config.Destination
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
		source := w.resources.sources[id]
		registration := registered[id]
		topics[registration.Names.Topic] = source
		workers[registration.Names.Topic] = w.worker(input, source, registration, dispatch)
	}
	return kafka.NewConsumer(ctx, kafka.ConsumerOptions{
		Brokers:     w.engine.Kafka.Brokers,
		Scope:       w.resources.scope,
		Destination: destination.ID,
		Sources:     topics,
		Admin:       w.resources.kafka,
		Store:       w.resources.store,
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
			return debezium.Decode(source, registration.Identity, w.resources.scope.Generation, raw)
		},
		Store:       w.resources.store,
		Scope:       w.resources.scope,
		Destination: input.ID,
		SourceID:    source.ID,
		Gate:        dispatch,
		Log:         w.log,
	}
}

// Close releases the resources this App's adapters hold.
func (a *App) Close() { a.resources.Close() }

// Run starts an engine under a bounded startup timeout, then serves the
// operator API until ctx is canceled or the server itself fails. On
// shutdown the worker is stopped before the HTTP server is drained, so no
// in-flight delivery is cut short by the server closing first.
// http.ErrServerClosed from a clean Shutdown is reported as a nil error.
func Run(ctx context.Context, engine config.Engine, application config.Application, token string, generation stream.Generation) error {
	initCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	r, err := New(initCtx, engine, application, generation, os.Stdout)
	cancel()
	if err != nil {
		return err
	}
	defer r.Close()
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan struct{})
	go func() { defer close(done); r.Start(workerCtx) }()
	server := &http.Server{Addr: engine.Operator.Listen, Handler: operator.New(token, r).Handler(), ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 40 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()
	select {
	case <-ctx.Done():
	case err = <-serveErr:
	}
	stop()
	<-done
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), engine.Delivery.DrainTimeout)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
