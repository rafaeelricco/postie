package debezium

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/rafaeelricco/postie/internal/provision"
)

// connectStatus is the Kafka Connect status response for one connector,
// decoded before statusFrom reduces it to provision.Status.
type connectStatus struct {
	Connector struct {
		State provision.ConnectorState `json:"state"`
		Trace string                   `json:"trace"`
	} `json:"connector"`
	Tasks []struct {
		State provision.ConnectorState `json:"state"`
		Trace string                   `json:"trace"`
	} `json:"tasks"`
}

// statusFrom reduces a raw Connect status to provision.Status: failed if the
// connector or any task reports StateFailed, with the first non-empty trace.
func statusFrom(raw connectStatus) provision.Status {
	status := provision.Status{Connector: raw.Connector.State}
	failed := raw.Connector.State == provision.StateFailed
	trace := raw.Connector.Trace
	for _, task := range raw.Tasks {
		status.Tasks = append(status.Tasks, task.State)
		if task.State == provision.StateFailed {
			failed = true
			if trace == "" {
				trace = task.Trace
			}
		}
	}
	status.Failed = failed
	status.Trace = trace
	return status
}

// ConnectorStatus fetches and reduces the status of this client's connector
// named name.
func (c Client) ConnectorStatus(ctx context.Context, name string) (provision.Status, error) {
	return ConnectorStatus(ctx, c.HTTP, c.URL, name)
}

// ConnectorStatus fetches the status of one connector.
func ConnectorStatus(ctx context.Context, client *http.Client, connectURL, name string) (provision.Status, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, connectorURL(connectURL, name, "status"), nil)
	if err != nil {
		return provision.Status{}, fmt.Errorf("capture: build status request for %q: %w", name, err)
	}
	resp, err := do(client, req)
	if err != nil {
		return provision.Status{}, fmt.Errorf("capture: get connector status for %q: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return provision.Status{}, provision.ErrConnectorMissing
	}
	if resp.StatusCode != http.StatusOK {
		return provision.Status{}, fmt.Errorf("capture: connect returned %d for connector %q status: %s", resp.StatusCode, name, errorBody(resp))
	}
	var raw connectStatus
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return provision.Status{}, fmt.Errorf("capture: decode connector status for %q: %w", name, err)
	}
	return statusFrom(raw), nil
}
