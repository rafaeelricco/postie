package debezium

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/rafaeelricco/postie/internal/provision"
	"github.com/rafaeelricco/postie/internal/stream"
)

// Connection contains source database settings used in a connector config.
type Connection struct {
	Host     string
	Port     int
	Username string
	Password string
	Database string
}

// Client manages connectors through one Kafka Connect endpoint.
type Client struct {
	URL         string
	HTTP        *http.Client
	Connections map[string]Connection
}

// ConnectorConfig builds the literal Debezium connector configuration for a
// source, frozen identity, and stream names.
func ConnectorConfig(src stream.Source, connection Connection, id stream.Identity, n stream.Names, publication provision.PublicationMode) map[string]string {
	columnNames := make([]string, len(id.Columns))
	includeList := make([]string, len(id.Columns))
	for i, c := range id.Columns {
		columnNames[i] = quoteIdent(c.Name)
		includeList[i] = regexp.QuoteMeta("public." + src.Table + "." + c.Name)
	}
	snapshotSelect := fmt.Sprintf(
		"SELECT %s FROM public.%s ORDER BY %s ASC",
		strings.Join(columnNames, ", "), quoteIdent(src.Table), quoteIdent(src.SerialColumn),
	)
	return map[string]string{
		"connector.class":                     "io.debezium.connector.postgresql.PostgresConnector",
		"plugin.name":                         "pgoutput",
		"database.hostname":                   connection.Host,
		"database.port":                       strconv.Itoa(connection.Port),
		"database.user":                       connection.Username,
		"database.password":                   connection.Password,
		"database.dbname":                     connection.Database,
		"topic.prefix":                        n.TopicPrefix,
		"table.include.list":                  "public." + src.Table,
		"column.include.list":                 strings.Join(includeList, ","),
		"message.key.columns":                 "public." + src.Table + ":" + src.PartitioningColumn,
		"slot.name":                           n.Slot,
		"publication.name":                    n.Publication,
		"publication.autocreate.mode":         string(publication),
		"snapshot.mode":                       "initial",
		"snapshot.select.statement.overrides": "public." + src.Table,
		"snapshot.select.statement.overrides.public." + src.Table: snapshotSelect,
		"skipped.operations":   "none",
		"tombstones.on.delete": "false",
		"binary.handling.mode": "base64", "decimal.handling.mode": "string",
		"time.precision.mode":                       "adaptive_time_microseconds",
		"heartbeat.interval.ms":                     "10000",
		"topic.creation.default.replication.factor": "-1",
		"topic.creation.default.partitions":         "1",
		"key.converter":                             "org.apache.kafka.connect.json.JsonConverter", "key.converter.schemas.enable": "false",
		"value.converter": "org.apache.kafka.connect.json.JsonConverter", "value.converter.schemas.enable": "false",
		"producer.override.acks": "all",
	}
}

// quoteIdent double-quotes a Postgres identifier so it can appear literally
// in a generated SQL statement.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// EnsureConnector idempotently applies the connector configuration.
func (c Client) EnsureConnector(ctx context.Context, src stream.Source, id stream.Identity, n stream.Names, publication provision.PublicationMode) error {
	connection := c.Connections[src.ID]
	return EnsureConnector(ctx, c.HTTP, c.URL, n, ConnectorConfig(src, connection, id, n, publication))
}

// EnsureConnector applies a literal connector configuration through Kafka
// Connect. The request body is never included in errors because it contains
// database credentials.
func EnsureConnector(ctx context.Context, client *http.Client, connectURL string, n stream.Names, cfg map[string]string) error {
	body, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("capture: marshal connector config: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, connectorURL(connectURL, n.Connector, "config"), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("capture: build connector request for %q: %w", n.Connector, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := do(client, req)
	if err != nil {
		return fmt.Errorf("capture: put connector config for %q: %w", n.Connector, err)
	}
	defer resp.Body.Close()
	respBody := errorBody(resp) // read even on success so the connection can be reused
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		return nil
	}
	return fmt.Errorf("capture: connect returned %d for connector %q: %s", resp.StatusCode, n.Connector, respBody)
}
