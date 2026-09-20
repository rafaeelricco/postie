//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/adapters/controlpg"
	"github.com/rafaeelricco/postie/internal/config"
	"github.com/rafaeelricco/postie/internal/stream"
)

func TestNullableSQLJSONSnapshotAndLiveDelivery(t *testing.T) {
	table := UniqueNamespace(t)
	ExecOnSource(t, fmt.Sprintf(`CREATE TABLE %s (
		id BIGSERIAL PRIMARY KEY,
		correlation_id TEXT NOT NULL,
		json_value JSON
	)`, table))

	insertNullableJSONRow(t, table, "snapshot-null", nil)
	snapshotJSON := `{"state":"snapshot"}`
	insertNullableJSONRow(t, table, "snapshot-value", &snapshotJSON)

	source := HostSource(table)
	source.Columns = []string{"id", "correlation_id", "json_value"}
	source.SerialColumn = "id"
	source.PartitioningColumn = "correlation_id"
	_, _, identity, names := provisionSource(t, source)

	store, err := controlpg.Open(context.Background(), controlConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topic := waitTopicVisible(t, Admin(t), names.Topic, 15*time.Second)
	if err := store.RegisterStream(context.Background(), stream.Scope{
		Namespace: namespace, Environment: environment, Generation: 1,
	}, stream.Registration{
		SourceID: source.ID,
		Identity: identity,
		Names:    names,
		TopicID:  [16]byte(topic.ID),
	}); err != nil {
		t.Fatal(err)
	}

	receiver := NewReceiver(t)
	engine := config.Engine{}
	engine.Namespace = namespace
	engine.Environment = environment
	engine.Kafka.Brokers = []string{kafkaHostAddr}
	engine.Kafka.Partitions = partitions
	engine.Kafka.ReplicationFactor = replication
	engine.Connect.URL = connectURL
	engine.Control.DatabaseURL = controlConnString()
	engine.Delivery.RequestTimeout = time.Second
	engine.Delivery.DrainTimeout = 5 * time.Second
	destination := config.Destination{
		ID:       table + "_projection",
		Type:     config.DestinationHTTPPush,
		Endpoint: receiver.Server.URL,
		Username: "test",
		Password: "test",
		Sources:  []string{source.ID},
	}
	application := config.Application{
		Sources:      []config.Source{source},
		Destinations: []config.Destination{destination},
	}
	runtime, _ := startRuntime(t, engine, application)
	waitRuntime(t, func() bool {
		seen := receivedNullableJSONRows(receiver)
		return runtime.Status().Status == "ready" && seen["snapshot-null"] && seen["snapshot-value"]
	})

	insertNullableJSONRow(t, table, "live-null", nil)
	liveJSON := `{"state":"live"}`
	insertNullableJSONRow(t, table, "live-value", &liveJSON)
	waitRuntime(t, func() bool {
		seen := receivedNullableJSONRows(receiver)
		return seen["snapshot-null"] && seen["snapshot-value"] && seen["live-null"] && seen["live-value"]
	})

	want := map[string]struct {
		isNull bool
		value  string
	}{
		"snapshot-null":  {isNull: true},
		"snapshot-value": {value: snapshotJSON},
		"live-null":      {isNull: true},
		"live-value":     {value: liveJSON},
	}
	seen := make(map[string]bool, len(want))
	for _, request := range receiver.Received() {
		var envelope struct {
			Payload map[string]json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(request.Body, &envelope); err != nil {
			t.Fatalf("decode delivered envelope: %v", err)
		}
		var correlationID string
		if err := json.Unmarshal(envelope.Payload["correlation_id"], &correlationID); err != nil {
			t.Fatalf("decode correlation_id: %v", err)
		}
		expected, ok := want[correlationID]
		if !ok {
			t.Fatalf("unexpected delivered row %q", correlationID)
		}
		seen[correlationID] = true

		got := envelope.Payload["json_value"]
		if expected.isNull {
			if string(got) != "null" {
				t.Errorf("row %q json_value = %s, want JSON null", correlationID, got)
			}
			continue
		}
		var gotJSONText string
		if err := json.Unmarshal(got, &gotJSONText); err != nil {
			t.Errorf("row %q json_value = %s, want a JSON string containing SQL json text: %v", correlationID, got, err)
			continue
		}
		if gotJSONText != expected.value {
			t.Errorf("row %q json_value text = %q, want %q", correlationID, gotJSONText, expected.value)
		}
	}
	for correlationID := range want {
		if !seen[correlationID] {
			t.Errorf("row %q was not delivered", correlationID)
		}
	}
}

func receivedNullableJSONRows(receiver *Receiver) map[string]bool {
	seen := make(map[string]bool)
	for _, request := range receiver.Received() {
		var envelope struct {
			Payload map[string]json.RawMessage `json:"payload"`
		}
		if json.Unmarshal(request.Body, &envelope) != nil {
			continue
		}
		var correlationID string
		if json.Unmarshal(envelope.Payload["correlation_id"], &correlationID) == nil {
			seen[correlationID] = true
		}
	}
	return seen
}

func insertNullableJSONRow(t *testing.T, table, correlationID string, jsonText *string) {
	t.Helper()
	conn := SourceDB(t)
	var value any
	if jsonText != nil {
		value = *jsonText
	}
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		`INSERT INTO %s (correlation_id, json_value) VALUES ($1, $2::json)`, table,
	), correlationID, value); err != nil {
		t.Fatalf("insert row %q: %v", correlationID, err)
	}
}
