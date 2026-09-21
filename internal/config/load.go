package config

import (
	"fmt"
	"os"
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
