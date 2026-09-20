//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"

	engineapp "github.com/rafaeelricco/postie/internal/app"
	"github.com/rafaeelricco/postie/internal/config"
	provisioning "github.com/rafaeelricco/postie/internal/provision"

	// provision runs the whole InspectTable -> EnsureTopic -> EnsureConnector
	// sequence for table, registers its cleanup, and waits for the connector to
	// reach RUNNING before returning.
	"github.com/rafaeelricco/postie/internal/adapters/debezium"
	"github.com/rafaeelricco/postie/internal/adapters/kafka"
	streams "github.com/rafaeelricco/postie/internal/stream"
)

const (
	namespace   = "postie"
	environment = "integtest"
	partitions  = 3
	replication = 1
)

func provision(t *testing.T, table string) (hostSrc, containerSrc config.Source, id streams.Identity, names streams.Names) {
	t.Helper()
	return provisionSource(t, HostSource(table))
}

func provisionSource(t *testing.T, source config.Source) (hostSrc, containerSrc config.Source, id streams.Identity, names streams.Names) {
	t.Helper()
	hostSrc = source
	containerSrc = source
	containerSrc.Host = containerPostgresHost
	containerSrc.Port = containerPostgresPort

	var err error
	id, err = inspectTable(context.Background(), hostSrc, partitions)
	if err != nil {
		t.Fatalf("InspectTable: %v", err)
	}

	names = provisioning.NamesFor(namespace, environment, engineapp.Source(hostSrc), 1)
	CleanupStream(t, names)

	adm := Admin(t)
	if _, err := (&kafka.Admin{Client: adm}).EnsureTopic(context.Background(), names, partitions, replication); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}

	cfg := debezium.ConnectorConfig(engineapp.Source(containerSrc), connectorConnection(containerSrc), id, names, provisioning.PublicationManaged)
	if err := debezium.EnsureConnector(context.Background(), http.DefaultClient, connectURL, names, cfg); err != nil {
		t.Fatalf("EnsureConnector: %v", err)
	}

	waitConnectorRunning(t, names.Connector, 90*time.Second)
	return hostSrc, containerSrc, id, names
}

func allRunning(states []provisioning.ConnectorState) bool {
	for _, s := range states {
		if s != provisioning.StateRunning {
			return false
		}
	}
	return true
}

// waitConnectorRunning polls Connect until name's connector and all its
// tasks report RUNNING, fataling the test if it instead reports FAILED or
// never settles within timeout.
func waitConnectorRunning(t *testing.T, name string, timeout time.Duration) provisioning.Status {
	t.Helper()
	status := waitConnectorSettled(t, name, timeout)
	if status.Failed {
		t.Fatalf("connector %q failed: connector=%s tasks=%v\ntrace:\n%s", name, status.Connector, status.Tasks, status.Trace)
	}
	return status
}

// waitConnectorSettled polls Connect until name's connector is either
// running (connector + every task RUNNING) or failed (connector or any task
// FAILED), returning whichever comes first. It fatals the test only if
// neither happens within timeout.
func waitConnectorSettled(t *testing.T, name string, timeout time.Duration) provisioning.Status {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last provisioning.Status
	for time.Now().Before(deadline) {
		status, err := debezium.ConnectorStatus(context.Background(), http.DefaultClient, connectURL, name)
		if err == nil {
			last = status
			if status.Failed {
				return status
			}
			if status.Connector == provisioning.StateRunning && len(status.Tasks) > 0 && allRunning(status.Tasks) {
				return status
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("connector %q did not settle (running or failed) within %s (last status: %+v)", name, timeout, last)
	return provisioning.Status{}
}

// waitTopicVisible polls ListTopics until topic is visible with no error.
// Kafka's own metadata can lag briefly right after a CreateTopics response,
// even on the same broker that served it; this is a Kafka-side propagation
// delay, not a source adapter defect.
func waitTopicVisible(t *testing.T, adm *kadm.Client, topic string, timeout time.Duration) kadm.TopicDetail {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		details, err := adm.ListTopics(context.Background(), topic)
		if err != nil {
			lastErr = err
		} else if detail, ok := details[topic]; ok {
			if detail.Err == nil {
				return detail
			}
			lastErr = detail.Err
		} else {
			lastErr = fmt.Errorf("topic %q not present in metadata response", topic)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("topic %q never became visible within %s: %v", topic, timeout, lastErr)
	return kadm.TopicDetail{}
}

// TestSnapshotThenLiveInSerialOrder seeds 200 rows before provisioning (so
// Debezium's initial snapshot captures them), provisions the stream, then
// inserts 200 more rows live. For every correlation_id key, the captured
// "id" values must strictly ascend in topic order -- proving the snapshot
// and the live stream hand off without reordering or dropping rows.
func TestSnapshotThenLiveInSerialOrder(t *testing.T) {
	table := UniqueNamespace(t)
	CreateEventTable(t, table)

	correlationIDs := []string{"corr-a", "corr-b", "corr-c"}
	seeded := InsertEvents(t, table, 200, correlationIDs...)

	_, _, _, names := provision(t, table)

	live := InsertEvents(t, table, 200, correlationIDs...)

	want := len(seeded) + len(live)
	records := ConsumeAll(t, names.Topic, want, 3*time.Minute)

	lastIDPerKey := map[string]int64{}
	for _, r := range records {
		var key struct {
			CorrelationID string `json:"correlation_id"`
		}
		if err := json.Unmarshal(r.Key, &key); err != nil {
			t.Fatalf("record key %s is not valid JSON: %v", r.Key, err)
		}
		after := decodeAfter(t, r.Value)
		id := mustInt64(t, after["id"])

		if prev, ok := lastIDPerKey[key.CorrelationID]; ok && id <= prev {
			t.Fatalf("key %q: id %d did not increase after %d (partition=%d offset=%d)", key.CorrelationID, id, prev, r.Partition, r.Offset)
		}
		lastIDPerKey[key.CorrelationID] = id
	}
	if len(lastIDPerKey) != len(correlationIDs) {
		t.Fatalf("saw %d distinct correlation ids, want %d", len(lastIDPerKey), len(correlationIDs))
	}
}

// TestKeyIsPartitioningColumn asserts the Kafka record key is exactly
// {"correlation_id": <value>} and that a given key is always routed to the
// same partition.
func TestKeyIsPartitioningColumn(t *testing.T) {
	table := UniqueNamespace(t)
	CreateEventTable(t, table)

	correlationIDs := []string{"key-a", "key-b", "key-c"}
	ids := InsertEvents(t, table, 30, correlationIDs...)

	_, _, _, names := provision(t, table)

	records := ConsumeAll(t, names.Topic, len(ids), 90*time.Second)

	partitionForKey := map[string]int32{}
	for _, r := range records {
		var keyObj map[string]string
		if err := json.Unmarshal(r.Key, &keyObj); err != nil {
			t.Fatalf("record key is not a JSON object: %s: %v", r.Key, err)
		}
		if len(keyObj) != 1 {
			t.Fatalf("record key has %d fields, want exactly 1 (correlation_id): %s", len(keyObj), r.Key)
		}
		correlationID, ok := keyObj["correlation_id"]
		if !ok {
			t.Fatalf("record key missing \"correlation_id\": %s", r.Key)
		}
		if p, seen := partitionForKey[correlationID]; seen {
			if p != r.Partition {
				t.Fatalf("key %q was seen on both partition %d and partition %d", correlationID, p, r.Partition)
			}
		} else {
			partitionForKey[correlationID] = r.Partition
		}
	}
	if len(partitionForKey) != len(correlationIDs) {
		t.Fatalf("saw %d distinct keys, want %d", len(partitionForKey), len(correlationIDs))
	}
}

// TestDelayedCommitIsCaptured runs two overlapping transactions: A takes
// serial id 1 first but holds its transaction open; B takes id 2 and
// commits immediately; A commits later. Postgres's logical replication
// stream orders changes by commit LSN, not by when the row's id was
// allocated, so B's row must reach Kafka before A's despite A's smaller id.
// Both must arrive either way -- a delayed commit must never be lost.
func TestDelayedCommitIsCaptured(t *testing.T) {
	table := UniqueNamespace(t)
	CreateEventTable(t, table)

	_, _, _, names := provision(t, table)

	ctx := context.Background()
	connA := SourceDB(t)
	connB := SourceDB(t)
	insertSQL := fmt.Sprintf(insertEventSQL, table)

	if _, err := connA.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("tx A: BEGIN: %v", err)
	}
	var idA int64
	if err := connA.QueryRow(ctx, insertSQL, "evtA-"+table, "aggA", 1, "evtA-"+table, "delayed-"+table, "TestEvent", `{"who":"A"}`).Scan(&idA); err != nil {
		t.Fatalf("tx A: insert: %v", err)
	}

	if _, err := connB.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("tx B: BEGIN: %v", err)
	}
	var idB int64
	if err := connB.QueryRow(ctx, insertSQL, "evtB-"+table, "aggB", 1, "evtB-"+table, "delayed-"+table, "TestEvent", `{"who":"B"}`).Scan(&idB); err != nil {
		t.Fatalf("tx B: insert: %v", err)
	}
	if _, err := connB.Exec(ctx, "COMMIT"); err != nil {
		t.Fatalf("tx B: COMMIT: %v", err)
	}

	if idA >= idB {
		t.Fatalf("test setup invariant broken: want idA (%d) < idB (%d) since A inserted (allocated its serial) first", idA, idB)
	}

	time.Sleep(200 * time.Millisecond) // let B's commit settle well before A's later commit

	if _, err := connA.Exec(ctx, "COMMIT"); err != nil {
		t.Fatalf("tx A: COMMIT: %v", err)
	}

	records := ConsumeAll(t, names.Topic, 2, 90*time.Second)

	var offsetA, offsetB int64 = -1, -1
	for _, r := range records {
		after := decodeAfter(t, r.Value)
		switch mustInt64(t, after["id"]) {
		case idA:
			offsetA = r.Offset
		case idB:
			offsetB = r.Offset
		}
	}
	if offsetA < 0 {
		t.Fatalf("row committed by tx A (id=%d) never arrived", idA)
	}
	if offsetB < 0 {
		t.Fatalf("row committed by tx B (id=%d) never arrived", idB)
	}
	if !(offsetB < offsetA) {
		t.Fatalf("expected tx B (id=%d, committed first) at an earlier offset than tx A (id=%d, committed later), got offsetB=%d offsetA=%d", idB, idA, offsetB, offsetA)
	}
}

// TestProvisionIsIdempotent runs InspectTable -> EnsureTopic ->
// EnsureConnector twice with identical inputs; the second pass must change
// nothing (same identity, same topic ID, connector still RUNNING).
func TestProvisionIsIdempotent(t *testing.T) {
	table := UniqueNamespace(t)
	CreateEventTable(t, table)

	hostSrc := HostSource(table)
	containerSrc := ContainerSource(table)

	id1, err := inspectTable(context.Background(), hostSrc, partitions)
	if err != nil {
		t.Fatalf("InspectTable (1st): %v", err)
	}

	names := provisioning.NamesFor(namespace, environment, engineapp.Source(hostSrc), 1)
	CleanupStream(t, names)

	adm := Admin(t)
	topicID1, err := (&kafka.Admin{Client: adm}).EnsureTopic(context.Background(), names, partitions, replication)
	if err != nil {
		t.Fatalf("EnsureTopic (1st): %v", err)
	}
	cfg1 := debezium.ConnectorConfig(engineapp.Source(containerSrc), connectorConnection(containerSrc), id1, names, provisioning.PublicationManaged)
	if err := debezium.EnsureConnector(context.Background(), http.DefaultClient, connectURL, names, cfg1); err != nil {
		t.Fatalf("EnsureConnector (1st): %v", err)
	}
	waitConnectorRunning(t, names.Connector, 90*time.Second)

	// Second pass, same table/identity/names: must be a pure no-op.
	id2, err := inspectTable(context.Background(), hostSrc, partitions)
	if err != nil {
		t.Fatalf("InspectTable (2nd): %v", err)
	}
	if fmt.Sprintf("%+v", id2) != fmt.Sprintf("%+v", id1) {
		t.Fatalf("InspectTable is not stable across runs:\n1st: %+v\n2nd: %+v", id1, id2)
	}

	topicID2, err := (&kafka.Admin{Client: adm}).EnsureTopic(context.Background(), names, partitions, replication)
	if err != nil {
		t.Fatalf("EnsureTopic (2nd): %v", err)
	}
	if topicID1 != topicID2 {
		t.Fatalf("EnsureTopic returned a different topic ID on the 2nd run: %x != %x", topicID1, topicID2)
	}

	cfg2 := debezium.ConnectorConfig(engineapp.Source(containerSrc), connectorConnection(containerSrc), id2, names, provisioning.PublicationManaged)
	if err := debezium.EnsureConnector(context.Background(), http.DefaultClient, connectURL, names, cfg2); err != nil {
		t.Fatalf("EnsureConnector (2nd): %v", err)
	}
	waitConnectorRunning(t, names.Connector, 90*time.Second)
}

// TestInspectTableRejections is the wiring check for readTableFacts against
// a real Postgres: one rejection (surfaced as a *capture.ContractError,
// proving readTableFacts's facts flow correctly into identityFrom) and one
// success. The full rule matrix is provision's TestIdentityFromRejections,
// which needs no Docker-free table.
func TestInspectTableRejections(t *testing.T) {
	t.Run("nullable-serial-is-a-contract-error", func(t *testing.T) {
		table := UniqueNamespace(t)
		ExecOnSource(t, fmt.Sprintf(`CREATE TABLE %s (id BIGINT, correlation_id TEXT NOT NULL)`, table))

		src := HostSource(table)
		src.Columns = []string{"id", "correlation_id"}
		src.SerialColumn = "id"
		src.PartitioningColumn = "correlation_id"

		_, err := inspectTable(context.Background(), src, partitions)
		if err == nil {
			t.Fatal("InspectTable succeeded, want an error")
		}
		var contractErr *provisioning.ContractError
		if !errors.As(err, &contractErr) {
			t.Fatalf("InspectTable error = %v (%T), want *capture.ContractError", err, err)
		}
		if !strings.Contains(contractErr.Reason, "must be NOT NULL") {
			t.Fatalf("ContractError.Reason = %q, want substring %q", contractErr.Reason, "must be NOT NULL")
		}
	})

	t.Run("valid-table-succeeds", func(t *testing.T) {
		table := UniqueNamespace(t)
		CreateEventTable(t, table)

		src := HostSource(table)
		id, err := inspectTable(context.Background(), src, partitions)
		if err != nil {
			t.Fatalf("InspectTable: %v", err)
		}
		if id.Table != table {
			t.Errorf("Identity.Table = %q, want %q", id.Table, table)
		}
		if id.Partitions != partitions {
			t.Errorf("Identity.Partitions = %d, want %d", id.Partitions, partitions)
		}
		if len(id.Columns) != len(src.Columns) {
			t.Errorf("Identity.Columns has %d entries, want %d", len(id.Columns), len(src.Columns))
		}
	})
}

// TestTopicConfig checks EnsureTopic's literal topic configuration
// (cleanup.policy, retention.*, partition count) directly against Kafka.
func TestTopicConfig(t *testing.T) {
	table := UniqueNamespace(t)
	hostSrc := HostSource(table)
	names := provisioning.NamesFor(namespace, environment, engineapp.Source(hostSrc), 1)
	CleanupStream(t, names)

	const wantPartitions = 4
	adm := Admin(t)
	if _, err := (&kafka.Admin{Client: adm}).EnsureTopic(context.Background(), names, wantPartitions, 1); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}

	detail := waitTopicVisible(t, adm, names.Topic, 15*time.Second)
	if len(detail.Partitions) != wantPartitions {
		t.Fatalf("topic has %d partitions, want %d", len(detail.Partitions), wantPartitions)
	}

	resourceConfigs, err := adm.DescribeTopicConfigs(context.Background(), names.Topic)
	if err != nil {
		t.Fatalf("DescribeTopicConfigs: %v", err)
	}
	rc, err := resourceConfigs.On(names.Topic, nil)
	if err != nil {
		t.Fatalf("resourceConfigs.On(%q): %v", names.Topic, err)
	}
	got := map[string]string{}
	explicit := map[string]bool{}
	for _, c := range rc.Configs {
		got[c.Key] = c.MaybeValue()
		explicit[c.Key] = c.Source == kmsg.ConfigSourceDynamicTopicConfig
	}
	want := map[string]string{
		"cleanup.policy":  "delete",
		"retention.ms":    "-1",
		"retention.bytes": "-1",
	}
	for key, value := range want {
		if !explicit[key] {
			t.Errorf("topic config %q was not set as a dynamic per-topic config (source=%v)", key, got[key])
		}
		if got[key] != value {
			t.Errorf("topic config %q = %q, want %q", key, got[key], value)
		}
	}
	// Our test cluster is single-broker, so replication factor 3 (which is
	// what turns on min.insync.replicas=2) cannot be exercised here; that
	// branch is only reachable with replication>=3, see kafka/topics.go's
	// topicConfigs. At replication 1, EnsureTopic must not have set it
	// explicitly (the cluster default still shows up in the describe, but
	// its Source must not be DYNAMIC_TOPIC_CONFIG).
	if explicit["min.insync.replicas"] {
		t.Errorf("min.insync.replicas should not be set as a dynamic per-topic config at replication factor 1, got %q", got["min.insync.replicas"])
	}
}

// TestProvisionWithNonOwnerReplicationUser verifies that the external
// publication path works when a replication role can read, but does not own,
// the source table. The database owner creates the publication before the
// connector starts.
func TestProvisionWithNonOwnerReplicationUser(t *testing.T) {
	table := UniqueNamespace(t)
	CreateEventTable(t, table)

	role := "repl_" + table
	const rolePassword = "replpass"
	ExecOnSource(t, fmt.Sprintf(`CREATE ROLE %s WITH REPLICATION LOGIN PASSWORD '%s'`, role, rolePassword))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, sourceConnString())
		if err != nil {
			t.Logf("cleanup: connect to source db: %v", err)
			return
		}
		defer conn.Close(ctx)
		// DROP ROLE fails while the role still has privileges GRANTed to it
		// (e.g. GRANT CONNECT ON DATABASE), so revoke/drop everything it
		// owns or was granted first.
		if _, err := conn.Exec(ctx, "DROP OWNED BY "+role); err != nil {
			t.Logf("cleanup: drop owned by %s: %v", role, err)
		}
		if _, err := conn.Exec(ctx, "DROP ROLE IF EXISTS "+role); err != nil {
			t.Logf("cleanup: drop role %s: %v", role, err)
		}
	})
	// The role gets REPLICATION LOGIN, CONNECT on the database, and SELECT
	// on the table -- nothing else. In particular it never owns the table
	// or the publication.
	ExecOnSource(t, fmt.Sprintf(`GRANT CONNECT ON DATABASE %s TO %s`, postgresDatabase, role))
	ExecOnSource(t, fmt.Sprintf(`GRANT SELECT ON TABLE %s TO %s`, table, role))

	hostSrc := HostSource(table)
	hostSrc.Username, hostSrc.Password = role, rolePassword
	containerSrc := ContainerSource(table)
	containerSrc.Username, containerSrc.Password = role, rolePassword

	names := provisioning.NamesFor(namespace, environment, engineapp.Source(hostSrc), 1)
	CleanupStream(t, names) // also drops the publication created below

	// The table OWNER creates the publication first -- a non-owner role
	// could never do this for itself under filtered mode.
	ExecOnSource(t, fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE %s`, names.Publication, table))

	id, err := inspectTable(context.Background(), hostSrc, partitions)
	if err != nil {
		t.Fatalf("InspectTable as non-owner replication role: %v", err)
	}

	adm := Admin(t)
	if _, err := (&kafka.Admin{Client: adm}).EnsureTopic(context.Background(), names, partitions, replication); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}

	cfg := debezium.ConnectorConfig(engineapp.Source(containerSrc), connectorConnection(containerSrc), id, names, provisioning.PublicationExternal)
	if err := debezium.EnsureConnector(context.Background(), http.DefaultClient, connectURL, names, cfg); err != nil {
		t.Fatalf("EnsureConnector: %v", err)
	}

	waitConnectorRunning(t, names.Connector, 90*time.Second)
	assertCaptureWorks(t, table, names)
}

func assertCaptureWorks(t *testing.T, table string, names streams.Names) {
	t.Helper()
	ids := InsertEvents(t, table, 3, "non-owner-capture-"+table)
	records := ConsumeAll(t, names.Topic, len(ids), 60*time.Second)

	got := map[int64]bool{}
	for _, r := range records {
		after := decodeAfter(t, r.Value)
		got[mustInt64(t, after["id"])] = true
	}
	for _, id := range ids {
		if !got[id] {
			t.Fatalf("row id=%d was never captured after enabling capture for the non-owner role", id)
		}
	}
}
