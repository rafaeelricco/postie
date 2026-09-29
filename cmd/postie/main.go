package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rafaeelricco/postie/internal/app"
	"github.com/rafaeelricco/postie/internal/config"
	"github.com/rafaeelricco/postie/internal/stream"
)

const usage = `usage: postie [--config POSTIE.yaml] [--generation N]
       postie config validate --config POSTIE.yaml
       postie provision --config POSTIE.yaml [--generation N]
`

const perSourceTimeout = 60 * time.Second

// main serves when the first argument is absent or a flag, and otherwise
// runs the named subcommand.
func main() {
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
	}
	serve()
}

// serve runs the engine until SIGINT or SIGTERM.
func serve() {
	configPath := flag.String("config", "postie.yaml", "Postie configuration")
	generation := flag.Int("generation", 1, "stream generation")
	flag.Parse()
	engine, application, err := config.Load(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	token, err := config.OperatorToken(os.Getenv("POSTIE_OPERATOR_TOKEN"), engine.Operator.TokenFile, os.ReadFile)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, engine, application, token, stream.Generation(*generation)); err != nil {
		log.Fatal(err)
	}
}

// run implements the postie CLI. It never calls os.Exit itself so it can
// be exercised directly from tests.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "config":
		if len(args) < 2 || args[1] != "validate" {
			fmt.Fprint(stderr, usage)
			return 2
		}
		return runConfigValidate(args[2:], stdout, stderr)
	case "provision":
		return runProvision(args[1:], stdout, stderr)
	default:
		fmt.Fprint(stderr, usage)
		return 2
	}
}

func runConfigValidate(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("validate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("config", "postie.yaml", "Postie configuration")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	_, app, err := config.Load(*path)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "valid: %d PostgreSQL sources, %d HTTP destinations\n", len(app.Sources), len(app.Destinations))
	return 0
}

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
