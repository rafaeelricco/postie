package config

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

// LoadApplication reads and validates the application configuration.
func LoadApplication(path string) (Application, error) {
	var c Application
	if err := load(path, &c); err != nil {
		return c, err
	}
	return c, c.Validate()
}

// LoadEngine reads the engine configuration, applies defaults, and validates it.
func LoadEngine(path string) (Engine, error) {
	var c Engine
	if err := load(path, &c); err != nil {
		return c, err
	}
	return c, c.Validate()
}

// Load reads the engine configuration at path, then the application
// configuration it points at. Both come back validated, so a caller never
// holds an engine whose application file was not checked.
//
//	engine, application, err := config.Load("postie.yaml")
func Load(path string) (Engine, Application, error) {
	engine, err := LoadEngine(path)
	if err != nil {
		return Engine{}, Application{}, err
	}
	application, err := LoadApplication(engine.ApplicationConfig)
	return engine, application, err
}

// load decodes a YAML file into target after expanding ${NAME} references.
// Expansion works on the parsed tree and not on the raw text, so a value can
// never inject YAML structure, and only string values are touched.
func load(path string, target any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var document yaml.Node
	if err := yaml.Unmarshal(b, &document); err != nil {
		return err
	}
	if err := expandScalars(stringScalars(&document), os.LookupEnv); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return document.Decode(target)
}

// stringScalars collects every string value in the tree. Mapping keys are
// skipped, so a key is never rewritten.
func stringScalars(n *yaml.Node) []*yaml.Node {
	switch n.Kind {
	case yaml.MappingNode:
		var scalars []*yaml.Node
		for i := 1; i < len(n.Content); i += 2 { // odd entries are the values
			scalars = append(scalars, stringScalars(n.Content[i])...)
		}
		return scalars
	case yaml.DocumentNode, yaml.SequenceNode:
		var scalars []*yaml.Node
		for _, child := range n.Content {
			scalars = append(scalars, stringScalars(child)...)
		}
		return scalars
	case yaml.ScalarNode:
		if n.ShortTag() == "!!str" {
			return []*yaml.Node{n}
		}
	}
	return nil
}

// expandScalars expands every scalar in place. It first checks all of them
// together, so one error names every missing variable in the file.
func expandScalars(scalars []*yaml.Node, lookup func(string) (string, bool)) error {
	values := make([]string, len(scalars))
	for i, scalar := range scalars {
		values[i] = scalar.Value
	}
	if _, err := ExpandEnvironment(strings.Join(values, "\n"), lookup); err != nil {
		return err
	}
	for _, scalar := range scalars {
		expanded, err := ExpandEnvironment(scalar.Value, lookup)
		if err != nil {
			return err
		}
		scalar.Value = expanded
	}
	return nil
}

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

const redactedPassword = "[REDACTED]"

// OperatorToken resolves the operator bearer token. A configured token file
// wins over the environment and must be readable; the result is trimmed and
// must not be empty.
//
//	token, err := config.OperatorToken(os.Getenv("POSTIE_OPERATOR_TOKEN"), engine.Operator.TokenFile, os.ReadFile)
func OperatorToken(env, file string, readFile func(string) ([]byte, error)) (string, error) {
	token := env
	if file != "" {
		b, err := readFile(file)
		if err != nil {
			return "", fmt.Errorf("operator token file: %w", err)
		}
		token = string(b)
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", fmt.Errorf("operator token is required: set POSTIE_OPERATOR_TOKEN or operator.token_file")
	}
	return token, nil
}

// Redacted returns a copy safe for logs and control-store revisions. It is a
// deep copy: changing the result never changes the receiver.
func (c Application) Redacted() Application {
	redacted := Application{
		Sources:      make([]Source, len(c.Sources)),
		Destinations: make([]Destination, len(c.Destinations)),
	}
	for i, s := range c.Sources {
		redacted.Sources[i] = redactSource(s)
	}
	for i, d := range c.Destinations {
		redacted.Destinations[i] = redactDestination(d)
	}
	return redacted
}

func redactSource(s Source) Source {
	s.Columns = cloneStrings(s.Columns)
	s.Password = redactPassword(s.Password)
	return s
}

func redactDestination(d Destination) Destination {
	d.Sources = cloneStrings(d.Sources)
	d.Password = redactPassword(d.Password)
	if d.Filter != nil {
		d.Filter = &Filter{Column: d.Filter.Column, Values: cloneStrings(d.Filter.Values)}
	}
	return d
}

// redactPassword hides a password but keeps "" as is, so the output still
// shows whether one was configured.
func redactPassword(password string) string {
	if password == "" {
		return ""
	}
	return redactedPassword
}

// cloneStrings copies a slice; nil and empty both become nil.
func cloneStrings(items []string) []string { return append([]string(nil), items...) }
