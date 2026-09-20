package config

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var environmentVariable = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// ExpandEnvironment replaces every ${NAME} using lookup. It reports all
// unset names, sorted and deduplicated, and substitutes nothing on error.
func ExpandEnvironment(s string, lookup func(string) (string, bool)) (string, error) {
	missing := map[string]struct{}{}
	for _, match := range environmentVariable.FindAllStringSubmatch(s, -1) {
		name := match[1]
		if _, ok := lookup(name); !ok {
			missing[name] = struct{}{}
		}
	}
	if len(missing) > 0 {
		names := make([]string, 0, len(missing))
		for name := range missing {
			names = append(names, name)
		}
		sort.Strings(names)
		if len(names) == 1 {
			return "", fmt.Errorf("environment variable %s is not set", names[0])
		}
		return "", fmt.Errorf("environment variables %s are not set", strings.Join(names, ", "))
	}
	out := environmentVariable.ReplaceAllStringFunc(s, func(match string) string {
		name := environmentVariable.FindStringSubmatch(match)[1]
		value, _ := lookup(name)
		return value
	})
	return out, nil
}
