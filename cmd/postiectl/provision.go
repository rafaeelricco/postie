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

func runProvision(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("provision", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("config", "postie.yaml", "Postie configuration")
	generationFlag := flags.Int("generation", 1, "stream generation")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	generation := stream.Generation(*generationFlag)
	if !generation.Valid() {
		fmt.Fprintln(stderr, "generation must be 1 or greater")
		return 2
	}
	engine, err := config.LoadEngine(*path)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	application, err := config.LoadApplication(engine.ApplicationConfig)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	controlCtx, controlCancel := context.WithTimeout(context.Background(), 5*time.Second)
	resources, err := app.Bootstrap(controlCtx, engine, application, generation)
	controlCancel()
	if err != nil {
		if initialization, ok := err.(*app.InitializationError); ok && initialization.Dependency == "Kafka" {
			fmt.Fprintln(stderr, err)
			return 1
		}
		for _, source := range application.Sources {
			fmt.Fprintf(stderr, "%s: %v\n", source.ID, err)
		}
		return 1
	}
	defer resources.Close()
	exitCode := 0
	for _, source := range application.Sources {
		ctx, cancel := context.WithTimeout(context.Background(), perSourceTimeout)
		registered, err := resources.Provision.ProvisionSource(ctx, resources.Sources[source.ID])
		cancel()
		if err != nil {
			fmt.Fprintf(stderr, "%s: %v\n", source.ID, err)
			exitCode = 1
			continue
		}
		names := registered.Names
		fmt.Fprintf(stdout, "provisioned source=%s generation=%s topic=%s connector=%s slot=%s publication=%s\n", source.ID, generation, names.Topic, names.Connector, names.Slot, names.Publication)
	}
	if exitCode == 0 {
		subscriptions := make([]string, len(application.Destinations))
		for i, destination := range application.Destinations {
			subscriptions[i] = destination.ID
		}
		if err := resources.Store.EnsureSubscriptions(context.Background(), resources.Scope, subscriptions); err != nil {
			fmt.Fprintf(stderr, "subscriptions: %v\n", err)
			return 1
		}
	}
	return exitCode
}
