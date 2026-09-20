package debezium

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/rafaeelricco/postie/internal/provision"
)

// decoded Connect responses: no HTTP server involved.
func TestStatusFrom(t *testing.T) {
	cases := []struct {
		name string
		json string
		want provision.Status
	}{
		{
			name: "connector-failed",
			json: `{"connector":{"state":"FAILED","trace":"boom"},"tasks":[]}`,
			want: provision.Status{Connector: provision.StateFailed, Failed: true, Trace: "boom"},
		},
		{
			name: "task-failed-with-trace",
			json: `{"connector":{"state":"RUNNING"},"tasks":[{"state":"RUNNING"},{"state":"FAILED","trace":"task exploded"}]}`,
			want: provision.Status{Connector: provision.StateRunning, Tasks: []provision.ConnectorState{provision.StateRunning, provision.StateFailed}, Failed: true, Trace: "task exploded"},
		},
		{
			name: "all-running",
			json: `{"connector":{"state":"RUNNING"},"tasks":[{"state":"RUNNING"},{"state":"RUNNING"}]}`,
			want: provision.Status{Connector: provision.StateRunning, Tasks: []provision.ConnectorState{provision.StateRunning, provision.StateRunning}},
		},
		{
			name: "no-tasks",
			json: `{"connector":{"state":"RUNNING"},"tasks":[]}`,
			want: provision.Status{Connector: provision.StateRunning},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var raw connectStatus
			if err := json.Unmarshal([]byte(c.json), &raw); err != nil {
				t.Fatalf("json.Unmarshal: %v", err)
			}
			got := statusFrom(raw)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("statusFrom = %+v, want %+v", got, c.want)
			}
		})
	}
}
