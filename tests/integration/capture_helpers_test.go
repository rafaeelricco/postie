//go:build integration

package integration

import (
	"context"
	"github.com/rafaeelricco/postie/internal/adapters/debezium"
	"github.com/rafaeelricco/postie/internal/adapters/sourcepg"
	engineapp "github.com/rafaeelricco/postie/internal/app"
	"github.com/rafaeelricco/postie/internal/config"
	provisioning "github.com/rafaeelricco/postie/internal/provision"
	"github.com/rafaeelricco/postie/internal/stream"
)

func inspectTable(ctx context.Context, input config.Source, partitions int32) (stream.Identity, error) {
	source := engineapp.Source(input)
	client := &sourcepg.Client{Source: source, Connection: sourcepg.Connection{Host: input.Host, Port: input.Port, Username: input.Username, Password: input.Password, Database: input.Database}}
	facts, err := client.InspectTable(ctx)
	if err != nil {
		return stream.Identity{}, err
	}
	return provisioning.IdentityFrom(source, facts, partitions)
}
func connectorConnection(input config.Source) debezium.Connection {
	return debezium.Connection{Host: input.Host, Port: input.Port, Username: input.Username, Password: input.Password, Database: input.Database}
}
