package config

import (
	"fmt"
	"strings"
)

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
