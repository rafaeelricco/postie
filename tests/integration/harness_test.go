//go:build integration

// Package integration drives the docker-compose infrastructure
// (postgres, control-postgres, kafka, connect) for tests that need the real
// thing: a Postgres source with logical replication, a real Kafka broker,
// and a real Debezium Connect worker. It never builds or starts the Postie
// service itself (that service sits behind the "postie" compose profile).
//
// Every test that provisions a stream (topic + connector + slot +
// publication) must pick a namespace from UniqueNamespace and register its
// cleanup with CleanupStream, so that topics, connectors, slots and
// publications from different tests never collide and never leak into the
// next run.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/rafaeelricco/postie/internal/config"
	streams "github.com/rafaeelricco/postie/internal/stream"
)

// Infrastructure endpoints, as published by tests/integration/compose.yaml. The test
// process (and the admin/pgx clients it builds) reach every service through
// its host-published port; only the connector config handed to Connect
// itself uses in-network hostnames (see ContainerSource).
const (
	composeFile    = "compose.yaml"
	composeProject = "postie-integration"

	hostPostgresHost      = "localhost"
	hostPostgresPort      = 25432
	containerPostgresHost = "postgres"
	containerPostgresPort = 5432
	postgresUser          = "postie"
	postgresPassword      = "postie"
	postgresDatabase      = "postie"
	controlHost           = "localhost"
	controlPort           = 25433
	controlUser           = "control"
	controlPassword       = "control"
	controlDatabase       = "control"

	kafkaHostAddr = "localhost:39092"
	connectURL    = "http://localhost:28083"
)

// eventTableColumns defines the synthetic source table column order, which
// is also the DDL CreateEventTable applies.
var eventTableColumns = []string{
	"id", "event_id", "aggregate_id", "aggregate_version",
	"causation_id", "correlation_id", "recorded_on",
	"event_name", "payload", "json_metadata",
}

const eventTableDDL = `CREATE TABLE %s (
	id BIGSERIAL NOT NULL,
	event_id TEXT NOT NULL UNIQUE,
	aggregate_id TEXT NOT NULL,
	aggregate_version BIGINT NOT NULL,
	causation_id TEXT NOT NULL,
	correlation_id TEXT NOT NULL,
	recorded_on TIMESTAMPTZ NOT NULL,
	event_name TEXT NOT NULL,
	payload TEXT NOT NULL,
	json_metadata TEXT NOT NULL,
	PRIMARY KEY (id)
);`

const insertEventSQL = `INSERT INTO %s
	(event_id, aggregate_id, aggregate_version, causation_id, correlation_id, recorded_on, event_name, payload, json_metadata)
	VALUES ($1, $2, $3, $4, $5, now(), $6, $7, '{}')
	RETURNING id`

// ---------------------------------------------------------------------
// docker compose lifecycle
// ---------------------------------------------------------------------

func composeArgs(args ...string) []string {
	return append([]string{"compose", "-p", composeProject, "-f", composeFile}, args...)
}

func runCompose(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "docker", composeArgs(args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %s: %w\n%s", strings.Join(args, " "), err, out)
	}
	return nil
}

// Up starts postgres, control-postgres, kafka and connect (never the engine
// service, which lives behind the "postie" compose profile) and waits for
// all four to report healthy. First run can take up to five minutes while
// images are pulled.
func Up() error {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	return runCompose(ctx, "up", "-d", "--wait", "--wait-timeout", "300",
		"postgres", "control-postgres", "kafka", "connect")
}

// Stop stops (without removing) a single service's container.
func Stop(svc string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return runCompose(ctx, "stop", svc)
}

// Start restarts a stopped service's container and waits for it to report
// healthy again.
func Start(svc string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := runCompose(ctx, "start", svc); err != nil {
		return err
	}
	return runCompose(ctx, "up", "-d", "--wait", "--wait-timeout", "120", svc)
}

// Kill force-stops a service's container (SIGKILL), simulating a crash
// rather than a graceful stop.
func Kill(svc string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return runCompose(ctx, "kill", svc)
}

// Down tears the whole compose project down. wipeVolumes also removes the
// named volumes (postgres-data, control-data, kafka-data), so the next Up
// starts from a clean slate.
func Down(wipeVolumes bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	args := []string{"down"}
	if wipeVolumes {
		args = append(args, "-v")
	}
	return runCompose(ctx, args...)
}

func TestMain(m *testing.M) {
	if err := Up(); err != nil {
		fmt.Fprintln(os.Stderr, "integration harness: Up failed:", err)
		os.Exit(1)
	}

	code := m.Run()

	if os.Getenv("POSTIE_KEEP") == "1" {
		fmt.Fprintln(os.Stderr, "integration harness: POSTIE_KEEP=1, leaving", composeProject, "running")
	} else if err := Down(true); err != nil {
		fmt.Fprintln(os.Stderr, "integration harness: Down failed:", err)
		if code == 0 {
			code = 1
		}
	}

	os.Exit(code)
}

// ---------------------------------------------------------------------
// Postgres source helpers
// ---------------------------------------------------------------------

// connString builds the host connection string used by the integration harness.
func connString(src config.Source) string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(src.Username, src.Password),
		Host:   fmt.Sprintf("%s:%d", src.Host, src.Port),
		Path:   "/" + src.Database,
	}
	return u.String()
}

func sourceConnString() string {
	return connString(config.Source{
		Host: hostPostgresHost, Port: hostPostgresPort,
		Username: postgresUser, Password: postgresPassword, Database: postgresDatabase,
	})
}

func controlConnString() string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(controlUser, controlPassword),
		Host:     fmt.Sprintf("%s:%d", controlHost, controlPort),
		Path:     "/" + controlDatabase,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}

// HostSource returns the config.Source an engine process or test harness
// running on the host would use to reach the source table: localhost, the
// published port. This is what InspectTable and SlotHealth use.
func HostSource(table string) config.Source {
	return config.Source{
		ID:                 table,
		Description:        "integration test source",
		Type:               "postgres",
		Host:               hostPostgresHost,
		Port:               hostPostgresPort,
		Username:           postgresUser,
		Password:           postgresPassword,
		Database:           postgresDatabase,
		Table:              table,
		Columns:            append([]string(nil), eventTableColumns...),
		SerialColumn:       "id",
		PartitioningColumn: "correlation_id",
	}
}

// ContainerSource returns the config.Source Debezium Connect (running
// inside the compose network) uses to reach the same table: the "postgres"
// service hostname, the in-network port. This is what goes into
// ConnectorConfig.
func ContainerSource(table string) config.Source {
	src := HostSource(table)
	src.Host = containerPostgresHost
	src.Port = containerPostgresPort
	return src
}

// SourceDB opens a fresh connection to the source database as the table
// owner (localhost:25432) and closes it when the test ends.
func SourceDB(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), sourceConnString())
	if err != nil {
		t.Fatalf("SourceDB: connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// ExecOnSource runs sql (already fully formed -- callers substitute table
// or role names themselves, since Postgres identifiers cannot be bind
// parameters) against the source database as the table owner.
func ExecOnSource(t *testing.T, sql string) {
	t.Helper()
	conn := SourceDB(t)
	if _, err := conn.Exec(context.Background(), sql); err != nil {
		t.Fatalf("ExecOnSource: %v\nSQL: %s", err, sql)
	}
}

// CreateEventTable creates a synthetic event table owned by the test user.
func CreateEventTable(t *testing.T, table string) {
	t.Helper()
	ExecOnSource(t, fmt.Sprintf(eventTableDDL, table))
}

var eventCounter atomic.Uint64

// InsertEvents inserts n synthetic event rows into table, cycling
// through correlationIDs (round robin) as each row's correlation_id. If no
// correlationIDs are given, every row shares one generated correlation ID.
// It returns the inserted rows' bigserial ids, in insertion order.
func InsertEvents(t *testing.T, table string, n int, correlationIDs ...string) []int64 {
	t.Helper()
	if len(correlationIDs) == 0 {
		correlationIDs = []string{fmt.Sprintf("corr-%s-%d", table, eventCounter.Add(1))}
	}
	conn := SourceDB(t)
	ctx := context.Background()
	sql := fmt.Sprintf(insertEventSQL, table)

	ids := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		correlationID := correlationIDs[i%len(correlationIDs)]
		seq := eventCounter.Add(1)
		eventID := fmt.Sprintf("%s-evt-%d", table, seq)

		var id int64
		err := conn.QueryRow(ctx, sql,
			eventID, "agg-"+table, i+1, eventID, correlationID,
			"TestEvent", fmt.Sprintf(`{"n":%d}`, i),
		).Scan(&id)
		if err != nil {
			t.Fatalf("InsertEvents: insert row %d into %s: %v", i, table, err)
		}
		ids = append(ids, id)
	}
	return ids
}

// ---------------------------------------------------------------------
// Kafka / Connect helpers
// ---------------------------------------------------------------------

var nsCounter atomic.Uint64

// UniqueNamespace returns a short, lowercase, Postgres-identifier-safe
// namespace unique to this test run, e.g. "t17371234560001". Tests use it
// both as the capture namespace and as the source table name, so that
// topics, connectors, slots, publications and tables from different tests
// never collide.
func UniqueNamespace(t *testing.T) string {
	t.Helper()
	n := nsCounter.Add(1)
	return fmt.Sprintf("t%d%04d", time.Now().UnixNano()%1_000_000_000, n%10000)
}

// KafkaClient builds a franz-go client against the HOST listener
// (localhost:39092) and closes it when the test ends.
func KafkaClient(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	all := append([]kgo.Opt{kgo.SeedBrokers(kafkaHostAddr)}, opts...)
	cl, err := kgo.NewClient(all...)
	if err != nil {
		t.Fatalf("KafkaClient: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// Admin builds a kadm client against the HOST listener.
func Admin(t *testing.T) *kadm.Client {
	t.Helper()
	return kadm.NewClient(KafkaClient(t))
}

// ConsumeAll consumes topic from its start and blocks until at least want
// records have arrived or timeout elapses (fataling the test in the latter
// case), then returns everything collected.
func ConsumeAll(t *testing.T, topic string, want int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	cl := KafkaClient(t,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var records []*kgo.Record
	for len(records) < want && ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		fetches.EachError(func(topic string, partition int32, err error) {
			t.Fatalf("ConsumeAll: fetch error on %s[%d]: %v", topic, partition, err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			records = append(records, r)
		})
	}
	if len(records) < want {
		t.Fatalf("ConsumeAll: got %d records from %q, want at least %d within %s", len(records), topic, want, timeout)
	}
	return records
}

// deleteConnectorBestEffort issues DELETE /connectors/<name>, ignoring a
// missing connector; used only from cleanup paths.
func deleteConnectorBestEffort(t *testing.T, name string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodDelete, connectURL+"/connectors/"+name, nil)
	if err != nil {
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Logf("cleanup: delete connector %s: %v", name, err)
		return
	}
	resp.Body.Close()
}

// CleanupStream registers a t.Cleanup that deletes n's connector and drops
// its replication slot and publication, using the source table's owner
// connection (dropping a slot or publication needs neither be the slot's
// creator, but does need REPLICATION or superuser/ownership, which the
// owner connection always has). Every test that provisions a stream must
// call this right after computing its Names.
func CleanupStream(t *testing.T, n streams.Names) {
	t.Helper()
	t.Cleanup(func() {
		deleteConnectorBestEffort(t, n.Connector)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, sourceConnString())
		if err != nil {
			t.Logf("cleanup: connect to source db: %v", err)
			return
		}
		defer conn.Close(ctx)

		// Connector deletion returns before its replication connection exits.
		// Retry slot cleanup rather than leaking an active slot into the next test.
		for {
			var exists bool
			err := conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)`, n.Slot).Scan(&exists)
			if err == nil && !exists {
				break
			}
			if err == nil {
				_, err = conn.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, n.Slot)
			}
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				t.Errorf("cleanup: drop replication slot %s: %v", n.Slot, err)
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if _, err := conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+n.Publication); err != nil {
			t.Logf("cleanup: drop publication %s: %v", n.Publication, err)
		}
	})
}

// ---------------------------------------------------------------------
// Recording HTTP receiver
// ---------------------------------------------------------------------

// ReceivedRequest is one POST captured by a Receiver.
type ReceivedRequest struct {
	Body   []byte
	Header http.Header
	At     time.Time
}

// Receiver is a recording httptest.Server: every request is appended to
// Received() (mutex-guarded), and answered by a scriptable reply function
// (default: 200 with a successful acknowledgement body).
type Receiver struct {
	Server *httptest.Server

	mu       sync.Mutex
	received []ReceivedRequest
	reply    func(ReceivedRequest) (status int, body string)
}

// NewReceiver starts a recording receiver and closes it when the test ends.
func NewReceiver(t *testing.T) *Receiver {
	t.Helper()
	rec := &Receiver{
		reply: func(ReceivedRequest) (int, string) { return http.StatusOK, `{"result":{"success":{}}}` },
	}
	rec.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rr := ReceivedRequest{Body: body, Header: r.Header.Clone(), At: time.Now()}

		rec.mu.Lock()
		rec.received = append(rec.received, rr)
		reply := rec.reply
		rec.mu.Unlock()

		status, respBody := reply(rr)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		w.Write([]byte(respBody))
	}))
	t.Cleanup(rec.Server.Close)
	return rec
}

// SetReply replaces the scriptable reply function.
func (r *Receiver) SetReply(fn func(ReceivedRequest) (status int, body string)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reply = fn
}

// Received returns a snapshot of every request captured so far.
func (r *Receiver) Received() []ReceivedRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ReceivedRequest, len(r.received))
	copy(out, r.received)
	return out
}

// decodeAfter parses value as a Debezium change envelope and returns its
// "after" object, decoding numbers as json.Number so int8/float8 precision
// (see docs/compatibility.md) is never silently narrowed to float64.
func decodeAfter(t *testing.T, value []byte) map[string]any {
	t.Helper()
	var envelope struct {
		After map[string]any `json:"after"`
		Op    string         `json:"op"`
	}
	dec := json.NewDecoder(bytes.NewReader(value))
	dec.UseNumber()
	if err := dec.Decode(&envelope); err != nil {
		t.Fatalf("decodeAfter: %v\nvalue: %s", err, value)
	}
	if envelope.After == nil {
		t.Fatalf("decodeAfter: envelope has no \"after\" (op=%q): %s", envelope.Op, value)
	}
	return envelope.After
}

func mustInt64(t *testing.T, v any) int64 {
	t.Helper()
	num, ok := v.(json.Number)
	if !ok {
		t.Fatalf("mustInt64: %v is a %T, not json.Number", v, v)
	}
	n, err := num.Int64()
	if err != nil {
		t.Fatalf("mustInt64: %v: %v", v, err)
	}
	return n
}
