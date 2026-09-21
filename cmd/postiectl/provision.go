package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/rafaeelricco/postie/internal/app"
	"github.com/rafaeelricco/postie/internal/config"
	"github.com/rafaeelricco/postie/internal/stream"
)

const perSourceTimeout = 60 * time.Second

// runProvision establishes capture for every configured source, then records
// the subscriptions. Exit codes: 0 all provisioned, 1 any failure, 2 bad usage.
func runProvision(args []string, stdout, stderr io.Writer) int {
	path, generation, ok := provisionFlags(args, stderr)
	if !ok {
		return 2
	}
	engine, application, err := config.Load(path)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	resources, err := bootstrap(engine, application, generation)
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

// provisionFlags parses the provision subcommand's flags into a config path
// and a Generation that is already known valid, so nothing downstream needs
// to check it again. ok is false on a parse error or an invalid generation,
// either of which is bad usage rather than a runtime failure.
func provisionFlags(args []string, stderr io.Writer) (path string, generation stream.Generation, ok bool) {
	flags := flag.NewFlagSet("provision", flag.ContinueOnError)
	flags.SetOutput(stderr)
	pathFlag := flags.String("config", "postie.yaml", "Postie configuration")
	generationFlag := flags.Int("generation", 1, "stream generation")
	if err := flags.Parse(args); err != nil {
		return "", 0, false
	}
	generation = stream.Generation(*generationFlag)
	if !generation.Valid() {
		fmt.Fprintln(stderr, "generation must be 1 or greater")
		return "", 0, false
	}
	return *pathFlag, generation, true
}

// bootstrap opens the adapters provisioning needs, bounded by a startup
// timeout separate from the per-source timeout used below.
func bootstrap(engine config.Engine, application config.Application, generation stream.Generation) (*app.Resources, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return app.Bootstrap(ctx, engine, application, generation)
}

// reportBootstrapFailure prints err once when the Kafka client could not be
// created, and once per source id for any other failure.
func reportBootstrapFailure(stderr io.Writer, sources []config.Source, err error) {
	if initialization, ok := err.(*app.InitializationError); ok && initialization.Dependency == app.DependencyKafka {
		fmt.Fprintln(stderr, err)
		return
	}
	for _, source := range sources {
		fmt.Fprintf(stderr, "%s: %v\n", source.ID, err)
	}
}

// provisionSources establishes capture for every source, trying each one
// even after an earlier failure, and reports whether all of them succeeded.
func provisionSources(resources *app.Resources, sources []config.Source, generation stream.Generation, stdout, stderr io.Writer) bool {
	ok := true
	for _, source := range sources {
		ctx, cancel := context.WithTimeout(context.Background(), perSourceTimeout)
		registered, err := resources.Provision.ProvisionSource(ctx, resources.Sources[source.ID])
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
func ensureSubscriptions(resources *app.Resources, ids []string, stderr io.Writer) int {
	if err := resources.Store.EnsureSubscriptions(context.Background(), resources.Scope, ids); err != nil {
		fmt.Fprintf(stderr, "subscriptions: %v\n", err)
		return 1
	}
	return 0
}
