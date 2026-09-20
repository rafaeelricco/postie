package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

func LoadApplication(path string) (Application, error) {
	var c Application
	if err := load(path, &c); err != nil {
		return c, err
	}
	return c, c.Validate()
}

func LoadEngine(path string) (Engine, error) {
	var c Engine
	if err := load(path, &c); err != nil {
		return c, err
	}
	if err := c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

func load(path string, target any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var document yaml.Node
	if err := yaml.Unmarshal(b, &document); err != nil {
		return err
	}
	var scalars []*yaml.Node
	var visit func(*yaml.Node)
	visit = func(n *yaml.Node) {
		switch n.Kind {
		case yaml.MappingNode:
			for i := 1; i < len(n.Content); i += 2 {
				visit(n.Content[i])
			}
		case yaml.DocumentNode, yaml.SequenceNode:
			for _, child := range n.Content {
				visit(child)
			}
		case yaml.ScalarNode:
			if n.ShortTag() == "!!str" {
				scalars = append(scalars, n)
			}
		}
	}
	visit(&document)
	values := make([]string, len(scalars))
	for i, scalar := range scalars {
		values[i] = scalar.Value
	}
	// Check all values first to retain aggregated missing-variable errors.
	if _, err := ExpandEnvironment(strings.Join(values, "\n"), os.LookupEnv); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	for _, scalar := range scalars {
		scalar.Value, err = ExpandEnvironment(scalar.Value, os.LookupEnv)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	return document.Decode(target)
}
