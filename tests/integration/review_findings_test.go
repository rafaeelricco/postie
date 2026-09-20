//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"gopkg.in/yaml.v3"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/rafaeelricco/postie/internal/adapters/controlpg"
	"github.com/rafaeelricco/postie/internal/adapters/debezium"
	"github.com/rafaeelricco/postie/internal/adapters/kafka"
	engineapp "github.com/rafaeelricco/postie/internal/app"
	"github.com/rafaeelricco/postie/internal/config"
	provisioning "github.com/rafaeelricco/postie/internal/provision"
	streams "github.com/rafaeelricco/postie/internal/stream"
)

func TestRegression_CaptureNamesKeepConnectorsIndependent(t *testing.T) {
	testNamespace := UniqueNamespace(t)
	tableA := UniqueNamespace(t) + "_a"
	tableB := UniqueNamespace(t) + "_b"
	CreateEventTable(t, tableA)
	CreateEventTable(t, tableB)
	idsA := InsertEvents(t, tableA, 1, "orders-a")
	idsB := InsertEvents(t, tableB, 1, "orders-b")

	sourceA := HostSource(tableA)
	sourceA.ID = "orders.v1"
	sourceB := HostSource(tableB)
	sourceB.ID = "orders-v1"
	containerA := ContainerSource(tableA)
	containerA.ID = sourceA.ID
	containerB := ContainerSource(tableB)
	containerB.ID = sourceB.ID

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	connectClient := &http.Client{Timeout: 30 * time.Second}
	identityA, err := inspectTable(ctx, sourceA, partitions)
	if err != nil {
		t.Fatalf("InspectTable A: %v", err)
	}
	identityB, err := inspectTable(ctx, sourceB, partitions)
	if err != nil {
		t.Fatalf("InspectTable B: %v", err)
	}
	namesA := provisioning.NamesFor(testNamespace, environment, engineapp.Source(sourceA), 1)
	namesB := provisioning.NamesFor(testNamespace, environment, engineapp.Source(sourceB), 1)
	CleanupStream(t, namesA)
	CleanupStream(t, namesB)
	adm := Admin(t)
	if _, err := (&kafka.Admin{Client: adm}).EnsureTopic(ctx, namesA, partitions, replication); err != nil {
		t.Fatalf("EnsureTopic A: %v", err)
	}
	if err := debezium.EnsureConnector(ctx, connectClient, connectURL, namesA, debezium.ConnectorConfig(engineapp.Source(containerA), connectorConnection(containerA), identityA, namesA, provisioning.PublicationManaged)); err != nil {
		t.Fatalf("EnsureConnector A: %v", err)
	}
	waitConnectorRunning(t, namesA.Connector, 90*time.Second)
	if _, err := (&kafka.Admin{Client: adm}).EnsureTopic(ctx, namesB, partitions, replication); err != nil {
		t.Fatalf("EnsureTopic B: %v", err)
	}
	if err := debezium.EnsureConnector(ctx, connectClient, connectURL, namesB, debezium.ConnectorConfig(engineapp.Source(containerB), connectorConnection(containerB), identityB, namesB, provisioning.PublicationManaged)); err != nil {
		t.Fatalf("EnsureConnector B: %v", err)
	}
	waitConnectorRunning(t, namesB.Connector, 90*time.Second)

	for field, pair := range map[string][2]string{
		"topic":       {namesA.Topic, namesB.Topic},
		"connector":   {namesA.Connector, namesB.Connector},
		"slot":        {namesA.Slot, namesB.Slot},
		"publication": {namesA.Publication, namesB.Publication},
	} {
		if pair[0] == pair[1] {
			t.Fatalf("%s names collided: %q", field, pair[0])
		}
	}

	cfgA := getConnectorConfig(t, namesA.Connector)
	cfgB := getConnectorConfig(t, namesB.Connector)
	assertConnectorConfigFor(t, cfgA, namesA, tableA)
	assertConnectorConfigFor(t, cfgB, namesB, tableB)
	assertRecordIDs(t, ConsumeAll(t, namesA.Topic, len(idsA), 90*time.Second), idsA)
	assertRecordIDs(t, ConsumeAll(t, namesB.Topic, len(idsB), 90*time.Second), idsB)
}

func TestRegression_ExistingTopicReplication(t *testing.T) {
	table := UniqueNamespace(t)
	source := HostSource(table)
	names := provisioning.NamesFor(namespace, environment, engineapp.Source(source), 1)
	CleanupStream(t, names)
	adm := Admin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	configs := map[string]*string{
		"cleanup.policy":      kadm.StringPtr("delete"),
		"retention.ms":        kadm.StringPtr("-1"),
		"retention.bytes":     kadm.StringPtr("-1"),
		"min.insync.replicas": kadm.StringPtr("2"),
	}
	created, err := adm.CreateTopic(ctx, partitions, replication, configs, names.Topic)
	if err != nil || created.Err != nil {
		t.Fatalf("CreateTopic: response=%+v err=%v", created, err)
	}
	before := waitTopicVisible(t, adm, names.Topic, 30*time.Second)
	assignments := topicAssignments(before)
	id := [16]byte(before.ID)
	if _, err := (&kafka.Admin{Client: adm}).EnsureTopic(ctx, names, partitions, 3); err == nil {
		t.Fatal("EnsureTopic accepted an existing topic with replication factor 1 when 3 was requested")
	}
	after := waitTopicVisible(t, adm, names.Topic, 30*time.Second)
	assertTopicUnchanged(t, after, id, assignments)
	if _, err := (&kafka.Admin{Client: adm}).EnsureTopic(ctx, names, partitions, replication); err != nil {
		t.Fatalf("EnsureTopic with matching replication factor: %v", err)
	}
}

func TestRegression_RegisteredTopicReplication(t *testing.T) {
	_, app, _, table := runtimeFixture(t)
	names := provisioning.NamesFor(namespace, environment, engineapp.Source(HostSource(table)), 1)
	adm := Admin(t)
	before := waitTopicVisible(t, adm, names.Topic, 30*time.Second)
	assignments := topicAssignments(before)
	id := [16]byte(before.ID)

	root := repositoryRoot(t)
	dir := t.TempDir()
	appPath := filepath.Join(dir, "application.yaml")
	enginePath := filepath.Join(dir, "postie.yaml")
	application := config.Application{
		Sources: app.Sources,
		Destinations: []config.Destination{{
			ID: "review_destination", Description: "review destination", Type: config.DestinationHTTPPush,
			Endpoint: "http://127.0.0.1:1", Username: "test", Sources: []string{table},
		}},
	}
	writeYAML(t, appPath, application)
	engineDoc := map[string]any{
		"version": 1, "namespace": namespace, "environment": environment,
		"application_config": appPath,
		"kafka":              map[string]any{"brokers": []string{kafkaHostAddr}, "partitions": partitions, "replication_factor": 3},
		"connect":            map[string]any{"url": connectURL},
		"operator":           map[string]any{"listen": "127.0.0.1:0", "token_file": ""},
		"control":            map[string]any{"database_url": controlConnString()},
		"delivery":           map[string]any{"request_timeout": "1s", "drain_timeout": "5s"},
	}
	writeYAML(t, enginePath, engineDoc)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "run", "./cmd/postiectl", "provision", "--config", enginePath, "--generation", "1")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("postiectl provision unexpectedly succeeded: %s", out)
	}
	assertReplicationDiagnostic(t, string(out))
	after := waitTopicVisible(t, adm, names.Topic, 30*time.Second)
	assertTopicUnchanged(t, after, id, assignments)

	store, err := controlpg.Open(context.Background(), controlConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stream, found, err := store.GetStream(context.Background(), streams.Scope{Namespace: namespace, Environment: environment, Generation: 1}, table)
	if err != nil || !found {
		t.Fatalf("GetStream found=%v err=%v", found, err)
	}
	if stream.Blocked != "" {
		t.Fatalf("replication mismatch blocked stream: %q", stream.Blocked)
	}
}

func TestRegression_RuntimeReplication(t *testing.T) {
	engine, app, receiver, table := runtimeFixture(t)
	engine.Kafka.ReplicationFactor = 3
	runtime, stop := startRuntime(t, engine, app)
	waitRuntime(t, func() bool {
		for _, source := range runtime.Status().Sources {
			if source.ID == table && source.State == "unavailable" {
				return true
			}
		}
		return false
	})
	if got := len(receiver.Received()); got != 0 {
		t.Fatalf("runtime delivered records while replication factor was incompatible: %d", got)
	}
	var sourceStatus string
	for _, source := range runtime.Status().Sources {
		if source.ID == table {
			sourceStatus = source.Error
		}
	}
	assertReplicationDiagnostic(t, sourceStatus)
	assertStreamUnblocked(t, table)
	stop()

	engine.Kafka.ReplicationFactor = replication
	runtime, _ = startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return runtime.Status().Status == "ready" && len(receiver.Received()) == 2 })
}

func TestRegression_MixedCaseSnapshot(t *testing.T) {
	table := "Mixed_" + UniqueNamespace(t)
	quoted := (pgx.Identifier{table}).Sanitize()
	CreateEventTable(t, quoted)
	ids := InsertEvents(t, quoted, 1)
	_, _, _, names := provision(t, table)
	records := ConsumeAll(t, names.Topic, len(ids), 90*time.Second)
	if len(records) != 1 {
		t.Fatalf("got %d records, want exactly one seeded snapshot row", len(records))
	}
	if got := mustInt64(t, decodeAfter(t, records[0].Value)["id"]); got != ids[0] {
		t.Fatalf("snapshot row id=%d, want seeded id=%d", got, ids[0])
	}
	var envelope struct {
		Op string `json:"op"`
	}
	if err := json.Unmarshal(records[0].Value, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Op != "r" {
		t.Fatalf("seeded row operation=%q, want snapshot operation %q", envelope.Op, "r")
	}
}

func getConnectorConfig(t *testing.T, name string) map[string]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, connectURL+"/connectors/"+name+"/config", nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET connector config %q: %v", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET connector config %q: status=%d body=%s", name, resp.StatusCode, body)
	}
	var cfg map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func assertConnectorConfigFor(t *testing.T, cfg map[string]string, names streams.Names, table string) {
	t.Helper()
	want := map[string]string{
		"table.include.list": "public." + table,
		"topic.prefix":       names.TopicPrefix,
		"slot.name":          names.Slot,
		"publication.name":   names.Publication,
	}
	for key, value := range want {
		if got := cfg[key]; got != value {
			t.Fatalf("connector config %q=%q, want %q", key, got, value)
		}
	}
}

func assertRecordIDs(t *testing.T, records []*kgo.Record, want []int64) {
	t.Helper()
	seen := make(map[int64]bool, len(records))
	for _, record := range records {
		seen[mustInt64(t, decodeAfter(t, record.Value)["id"])] = true
	}
	for _, id := range want {
		if !seen[id] {
			t.Fatalf("consumed records did not include seeded id %d", id)
		}
	}
}

func topicAssignments(detail kadm.TopicDetail) map[int32][]int32 {
	assignments := make(map[int32][]int32, len(detail.Partitions))
	for partition, value := range detail.Partitions {
		assignments[partition] = append([]int32(nil), value.Replicas...)
	}
	return assignments
}

func assertTopicUnchanged(t *testing.T, detail kadm.TopicDetail, id [16]byte, assignments map[int32][]int32) {
	t.Helper()
	if [16]byte(detail.ID) != id {
		t.Fatalf("topic UUID changed: before=%x after=%x", id, detail.ID)
	}
	if len(detail.Partitions) != len(assignments) {
		t.Fatalf("partition assignment count changed: before=%d after=%d", len(assignments), len(detail.Partitions))
	}
	for partition, want := range assignments {
		got, ok := detail.Partitions[partition]
		if !ok || !reflect.DeepEqual(got.Replicas, want) {
			t.Fatalf("partition %d replicas changed: before=%v after=%v", partition, want, got.Replicas)
		}
	}
}

func assertReplicationDiagnostic(t *testing.T, text string) {
	t.Helper()
	lower := strings.ToLower(text)
	for _, term := range []string{"has 1 replicas", "want 3", "topic", "partition"} {
		if !strings.Contains(lower, term) {
			t.Fatalf("replication diagnostic %q is missing %q", text, term)
		}
	}
}

func assertStreamUnblocked(t *testing.T, sourceID string) {
	t.Helper()
	store, err := controlpg.Open(context.Background(), controlConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stream, found, err := store.GetStream(context.Background(), streams.Scope{Namespace: namespace, Environment: environment, Generation: 1}, sourceID)
	if err != nil || !found {
		t.Fatalf("GetStream found=%v err=%v", found, err)
	}
	if stream.Blocked != "" {
		t.Fatalf("stream is blocked: %q", stream.Blocked)
	}
}

func writeYAML(t *testing.T, path string, value any) {
	t.Helper()
	b, err := yaml.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not locate repository root")
		}
		dir = parent
	}
}
