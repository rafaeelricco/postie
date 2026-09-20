package config

import (
	"fmt"
	"strings"
)

// OperatorToken resolves the operator bearer token. A configured token file
// wins over the environment and must be readable; the result is trimmed and
// must not be empty.
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

// Redacted returns a copy safe for logs and control-store revisions.
func (c Application) Redacted() Application {
	redacted := Application{
		Sources:      make([]Source, len(c.Sources)),
		Destinations: make([]Destination, len(c.Destinations)),
	}
	for i, s := range c.Sources {
		s.Columns = append([]string(nil), s.Columns...)
		if s.Password != "" {
			s.Password = "[REDACTED]"
		}
		redacted.Sources[i] = s
	}
	for i, d := range c.Destinations {
		d.Sources = append([]string(nil), d.Sources...)
		if d.Filter != nil {
			filter := *d.Filter
			filter.Values = append([]string(nil), d.Filter.Values...)
			d.Filter = &filter
		}
		if d.Password != "" {
			d.Password = "[REDACTED]"
		}
		redacted.Destinations[i] = d
	}
	return redacted
}
