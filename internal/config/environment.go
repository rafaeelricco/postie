package config

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

var environmentVariable = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// ExpandEnvironment replaces every ${NAME} using lookup. It reports all
// unset names, sorted and deduplicated, and substitutes nothing on error.
//
//	ExpandEnvironment("postgres://${DB_USER}@db", os.LookupEnv)
//	// "postgres://postie@db", nil
//	// "", environment variable DB_USER is not set
func ExpandEnvironment(s string, lookup func(string) (string, bool)) (string, error) {
	if missing := missingVariables(s, lookup); len(missing) > 0 {
		return "", missingError(missing)
	}
	return environmentVariable.ReplaceAllStringFunc(s, func(match string) string {
		value, _ := lookup(variableName(match))
		return value
	}), nil
}

// variableName extracts NAME from a "${NAME}" match.
func variableName(match string) string {
	return environmentVariable.FindStringSubmatch(match)[1]
}

// missingVariables lists the referenced names lookup does not know, sorted
// and without duplicates.
func missingVariables(s string, lookup func(string) (string, bool)) []string {
	var missing []string
	for _, match := range environmentVariable.FindAllStringSubmatch(s, -1) {
		if _, ok := lookup(match[1]); !ok {
			missing = append(missing, match[1])
		}
	}
	slices.Sort(missing)
	return slices.Compact(missing)
}

func missingError(names []string) error {
	if len(names) == 1 {
		return fmt.Errorf("environment variable %s is not set", names[0])
	}
	return fmt.Errorf("environment variables %s are not set", strings.Join(names, ", "))
}
