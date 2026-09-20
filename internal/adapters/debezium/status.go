package debezium

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/rafaeelricco/postie/internal/provision"
)

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

func (c Client) ConnectorStatus(ctx context.Context, name string) (provision.Status, error) {
	return ConnectorStatus(ctx, c.HTTP, c.URL, name)
}

// ConnectorStatus fetches the status of one connector.
func ConnectorStatus(ctx context.Context, client *http.Client, connectURL, name string) (provision.Status, error) {
	endpoint := strings.TrimRight(connectURL, "/") + "/connectors/" + name + "/status"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return provision.Status{}, fmt.Errorf("capture: build status request for %q: %w", name, err)
	}
	httpClient := client
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return provision.Status{}, fmt.Errorf("capture: get connector status for %q: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return provision.Status{}, provision.ErrConnectorMissing
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		return provision.Status{}, fmt.Errorf("capture: connect returned %d for connector %q status: %s", resp.StatusCode, name, string(respBody))
	}
	var raw connectStatus
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return provision.Status{}, fmt.Errorf("capture: decode connector status for %q: %w", name, err)
	}
	return statusFrom(raw), nil
}
