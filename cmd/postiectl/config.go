package main

import (
	"flag"
	"fmt"
	"io"

	"github.com/rafaeelricco/postie/internal/config"
)

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
