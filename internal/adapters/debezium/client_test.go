package debezium

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rafaeelricco/postie/internal/provision"
	"github.com/rafaeelricco/postie/internal/stream"
)

func TestClientAppliesConfigurationAndReadsRunningStatus(t *testing.T) {
	for _, code := range []int{http.StatusOK, http.StatusCreated} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			requests := make(chan string, 2)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- r.Method + " " + r.URL.Path
				if r.Method == http.MethodPut {
					var cfg map[string]string
					if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
						t.Error(err)
					}
					if r.Header.Get("Content-Type") != "application/json" || cfg["database.hostname"] != "db.internal" || cfg["database.password"] != testConnection().Password || cfg["slot.name"] != "existing_slot" || cfg["publication.autocreate.mode"] != "disabled" {
						t.Error("connector request lost its configured transport fields")
					}
					w.WriteHeader(code)
					return
				}
				fmt.Fprint(w, `{"connector":{"state":"RUNNING"},"tasks":[{"state":"RUNNING"}]}`)
			}))
			defer server.Close()
			source := testSource()
			// A nil HTTP client uses the default transport for both operations.
			client := Client{URL: server.URL + "/", Connections: map[string]Connection{source.ID: testConnection()}}
			names := stream.Names{Connector: "existing_connector", Slot: "existing_slot"}
			if err := client.EnsureConnector(context.Background(), source, testIdentity(source), names, provision.PublicationExternal); err != nil {
				t.Fatal(err)
			}
			status, err := client.ConnectorStatus(context.Background(), names.Connector)
			if err != nil || status.Failed || status.Connector != provision.StateRunning || len(status.Tasks) != 1 || status.Tasks[0] != provision.StateRunning {
				t.Fatalf("status = %+v, %v", status, err)
			}
			for _, want := range []string{"PUT /connectors/existing_connector/config", "GET /connectors/existing_connector/status"} {
				if got := <-requests; got != want {
					t.Fatalf("request=%q, want %q", got, want)
				}
			}
		})
	}
}

func TestClientRejectsMalformedConnectorEndpoint(t *testing.T) {
	client := Client{URL: "http://invalid\nendpoint", Connections: map[string]Connection{}}
	source := testSource()
	err := client.EnsureConnector(context.Background(), source, testIdentity(source), stream.Names{Connector: "existing"}, provision.PublicationManaged)
	if err == nil || !strings.Contains(err.Error(), "capture: build connector request") {
		t.Fatalf("malformed endpoint error = %v", err)
	}
}
