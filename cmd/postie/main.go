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

	"github.com/rafaeelricco/postie/internal/app"
	"github.com/rafaeelricco/postie/internal/config"
	"github.com/rafaeelricco/postie/internal/stream"
)

const usage = `usage: postie [--config POSTIE.yaml] [--generation N]
       postie config validate --config POSTIE.yaml
       postie provision --config POSTIE.yaml [--generation N]
`

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

// runProvision parses the flags and loads the configuration; app.Provision does
// the work. Exit codes: 0 all provisioned, 1 any failure, 2 bad usage.
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
	return app.Provision(engine, application, generation, stdout, stderr)
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
