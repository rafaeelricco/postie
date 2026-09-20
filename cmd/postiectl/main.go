package main

import (
	"fmt"
	"io"
	"os"
)

const usage = `usage: postiectl config validate --config POSTIE.yaml
       postiectl provision --config POSTIE.yaml [--generation N]
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run implements the postiectl CLI. It never calls os.Exit itself so it can
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
