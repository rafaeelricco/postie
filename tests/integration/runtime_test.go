//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"

	"github.com/rafaeelricco/postie/internal/adapters/controlpg"
	"github.com/rafaeelricco/postie/internal/adapters/kafka"
	engineapp "github.com/rafaeelricco/postie/internal/app"
	"github.com/rafaeelricco/postie/internal/config"
	"github.com/rafaeelricco/postie/internal/control"
	provisioning "github.com/rafaeelricco/postie/internal/provision"
	streams "github.com/rafaeelricco/postie/internal/stream"
)

func runtimeFixture(t *testing.T) (config.Engine, config.Application, *Receiver, string) {
	t.Helper()
	table := UniqueNamespace(t)
	CreateEventTable(t, table)
	InsertEvents(t, table, 2, "one-note")
	source, _, identity, names := provision(t, table)
	receiver := NewReceiver(t)
	var engine config.Engine
	engine.Namespace = namespace
	engine.Environment = environment
	engine.Kafka.Brokers = []string{kafkaHostAddr}
	engine.Kafka.Partitions = partitions
	engine.Kafka.ReplicationFactor = replication
	engine.Connect.URL = connectURL
	engine.Control.DatabaseURL = controlConnString()
	engine.Delivery.RequestTimeout = time.Second
	engine.Delivery.DrainTimeout = 5 * time.Second
	destination := config.Destination{ID: table + "_projection", Type: config.DestinationHTTPPush, Endpoint: receiver.Server.URL, Username: "test", Password: "test", Sources: []string{source.ID}}
	app := config.Application{Sources: []config.Source{source}, Destinations: []config.Destination{destination}}
	store, err := controlpg.Open(context.Background(), controlConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	details, err := Admin(t).ListTopics(context.Background(), names.Topic)
	if err != nil {
		t.Fatal(err)
	}
	err = store.RegisterStream(context.Background(), streams.Scope{Namespace: namespace, Environment: environment, Generation: 1}, streams.Registration{SourceID: source.ID, Identity: identity, Names: names, TopicID: [16]byte(details[names.Topic].ID)})
	if err != nil {
		t.Fatal(err)
	}
	return engine, app, receiver, table
}
func startRuntime(t *testing.T, engine config.Engine, app config.Application) (*engineapp.App, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	runtime, err := engineapp.New(ctx, engine, app, 1, io.Discard)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); runtime.Start(ctx) }()
	var stopped atomic.Bool
	stop := func() {
		if stopped.Swap(true) {
			return
		}
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("runtime shutdown timed out")
		}
		runtime.Close()
	}
	t.Cleanup(stop)
	return runtime, stop
}
func waitRuntime(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("runtime condition did not converge")
}
func TestRuntimeDeliveryPauseRestart(t *testing.T) {
	engine, app, receiver, table := runtimeFixture(t)
	runtime, stop := startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return len(receiver.Received()) >= 2 && runtime.Status().Status == "ready" })
	for _, req := range receiver.Received() {
		var envelope struct {
			Payload map[string]any `json:"payload"`
		}
		if err := json.Unmarshal(req.Body, &envelope); err != nil {
			t.Fatal(err)
		}
		recorded, ok := envelope.Payload["recorded_on"].(string)
		if !ok || recorded[len(recorded)-3:] != "+00" {
			t.Fatalf("SQL timestamp %v", envelope.Payload["recorded_on"])
		}
		if req.Header.Get("X-Postie-Event-ID") == "" {
			t.Fatal("missing diagnostic header")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sub, err := runtime.Change(ctx, app.Destinations[0].ID, control.StatePaused)
	if err != nil || sub.State != control.StatePaused {
		t.Fatalf("pause %+v %v", sub, err)
	}
	count := len(receiver.Received())
	InsertEvents(t, table, 1, "one-note")
	time.Sleep(time.Second)
	if len(receiver.Received()) != count {
		t.Fatal("delivery continued while paused")
	}
	stop()
	runtime, _ = startRuntime(t, engine, app)
	waitRuntime(t, func() bool {
		all := runtime.Subscriptions()
		return len(all) == 1 && all[0].State == control.StatePaused
	})
	if len(receiver.Received()) != count {
		t.Fatal("restart ignored persisted pause")
	}
	if _, err = runtime.Change(ctx, app.Destinations[0].ID, control.StateRunning); err != nil {
		t.Fatal(err)
	}
	waitRuntime(t, func() bool { return len(receiver.Received()) == count+1 })
	page, err := runtime.Logs("")
	if err != nil {
		t.Fatal(err)
	}
	committed := false
	for _, e := range page.Entries {
		if e.Message == "committed" && e.EventID != "" {
			committed = true
		}
	}
	waitRuntime(t, func() bool {
		page, _ := runtime.Logs("")
		for _, e := range page.Entries {
			if e.Message == "committed" && e.EventID != "" {
				return true
			}
		}
		return committed
	})
}
func TestRuntimeRetriesUntilAcknowledged(t *testing.T) {
	engine, app, receiver, _ := runtimeFixture(t)
	var attempts atomic.Int32
	receiver.SetReply(func(ReceivedRequest) (int, string) {
		if attempts.Add(1) <= 2 {
			return http.StatusServiceUnavailable, `{"error":"try again"}`
		}
		return http.StatusOK, `{"result":{"success":{}}}`
	})
	runtime, _ := startRuntime(t, engine, app)
	waitRuntime(t, func() bool {
		page, _ := runtime.Logs("")
		commits := 0
		for _, e := range page.Entries {
			if e.Message == "committed" {
				commits++
			}
		}
		return commits == 2
	})
	requests := receiver.Received()
	if len(requests) != 4 {
		t.Fatalf("attempts=%d", len(requests))
	}
	if requests[0].Header.Get("X-Postie-Event-ID") != requests[2].Header.Get("X-Postie-Event-ID") {
		t.Fatal("retry advanced partition")
	}
}

func TestRuntimeControlOutageStopsAndRecovers(t *testing.T) {
	engine, app, receiver, table := runtimeFixture(t)
	runtime, _ := startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return runtime.Status().Status == "ready" && len(receiver.Received()) == 2 })
	if err := Stop("control-postgres"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := Start("control-postgres"); err != nil {
			t.Error(err)
		}
	})
	waitRuntime(t, func() bool {
		all := runtime.Subscriptions()
		return !runtime.Status().Control && len(all) == 1 && all[0].State == control.StateBlocked
	})
	count := len(receiver.Received())
	InsertEvents(t, table, 1, "one-note")
	time.Sleep(time.Second)
	if len(receiver.Received()) != count {
		t.Fatal("delivery continued after control lease refresh failed")
	}
	if err := Start("control-postgres"); err != nil {
		t.Fatal(err)
	}
	waitRuntime(t, func() bool { return runtime.Status().Status == "ready" && len(receiver.Received()) == count+1 })
}

func TestRuntimePauseWaitsForAllWorkers(t *testing.T) {
	engine, app, receiver, table := runtimeFixture(t)
	first, _ := startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return first.Status().Status == "ready" && committedCount(first) == 2 })
	second, _ := startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return first.Status().Status == "ready" && second.Status().Status == "ready" })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := first.Change(ctx, app.Destinations[0].ID, control.StatePaused); err != nil {
		t.Fatal(err)
	}
	for _, r := range []*engineapp.App{first, second} {
		if subs := r.Subscriptions(); subs[0].State != control.StatePaused {
			t.Fatalf("pause returned before every worker exited: %+v", subs)
		}
	}
	before := len(receiver.Received())
	InsertEvents(t, table, 1, "one-note")
	time.Sleep(time.Second)
	if len(receiver.Received()) != before {
		t.Fatal("worker delivered while subscription was paused")
	}
	if _, err := first.Change(ctx, app.Destinations[0].ID, control.StateRunning); err != nil {
		t.Fatal(err)
	}
	waitRuntime(t, func() bool { return len(receiver.Received()) == before+1 })
}

func TestRuntimeInvalidCaptureEventsBlockSource(t *testing.T) {
	for _, operation := range []string{"update", "delete"} {
		t.Run(operation, func(t *testing.T) {
			engine, app, receiver, table := runtimeFixture(t)
			runtime, _ := startRuntime(t, engine, app)
			waitRuntime(t, func() bool {
				return len(receiver.Received()) >= 2 && runtime.Status().Status == "ready"
			})
			before := len(receiver.Received())

			switch operation {
			case "update":
				ExecOnSource(t, fmt.Sprintf("UPDATE %s SET payload = '{\"changed\":true}' WHERE id = (SELECT min(id) FROM %s)", table, table))
			case "delete":
				ExecOnSource(t, fmt.Sprintf("DELETE FROM %s WHERE id = (SELECT min(id) FROM %s)", table, table))
			}

			waitRuntime(t, func() bool {
				for _, source := range runtime.Status().Sources {
					if source.ID == table {
						return source.State == "blocked"
					}
				}
				return false
			})
			if got := len(receiver.Received()); got != before {
				t.Fatalf("invalid %s event advanced HTTP delivery: before=%d after=%d", operation, before, got)
			}
		})
	}
}

func TestRuntimeKeepGoingSkipSurvivesRestart(t *testing.T) {
	engine, app, receiver, _ := runtimeFixture(t)
	receiver.SetReply(func(ReceivedRequest) (int, string) {
		return http.StatusOK, `{"result":{"error":{"policy":"keep_going","class":"test","description":"skip"}}}`
	})
	runtime, stop := startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return committedCount(runtime) == 2 })
	before := len(receiver.Received())
	stop()

	runtime, _ = startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return runtime.Status().Status == "ready" })
	if got := len(receiver.Received()); got != before {
		t.Fatalf("restart redelivered audited skip: before=%d after=%d", before, got)
	}
}

func TestRuntimeRetryingPartitionDoesNotBlockAnother(t *testing.T) {
	engine, app, receiver, table := runtimeFixture(t)
	names := provisioning.NamesFor(namespace, environment, engineapp.Source(HostSource(table)), 1)
	var retryAttempts atomic.Int32
	receiver.SetReply(func(req ReceivedRequest) (int, string) {
		if req.Header.Get("X-Postie-Event-ID") == "retry-partition" {
			retryAttempts.Add(1)
			return http.StatusServiceUnavailable, `{"error":"retry"}`
		}
		return http.StatusOK, `{"result":{"success":{}}}`
	})
	runtime, _ := startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return runtime.Status().Status == "ready" && len(receiver.Received()) >= 2 })

	produceRuntimeRecord(t, names.Topic, 0, "retry-partition", "partition-zero", 10001)
	produceRuntimeRecord(t, names.Topic, 1, "other-partition", "partition-one", 10002)
	waitRuntime(t, func() bool {
		if retryAttempts.Load() < 2 {
			return false
		}
		return hasCommittedEvent(runtime, "other-partition")
	})
	if hasCommittedEvent(runtime, "retry-partition") {
		t.Fatal("retrying partition was committed")
	}
}

func TestRuntimeRevocationCancelsRetryWorker(t *testing.T) {
	engine, app, receiver, table := runtimeFixture(t)
	names := provisioning.NamesFor(namespace, environment, engineapp.Source(HostSource(table)), 1)
	var allow atomic.Bool
	receiver.SetReply(func(req ReceivedRequest) (int, string) {
		if strings.HasPrefix(req.Header.Get("X-Postie-Event-ID"), "revoked-partition-") && !allow.Load() {
			return 503, `{"error":"retry"}`
		}
		return 200, `{"result":{"success":{}}}`
	})
	runtime, _ := startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return runtime.Status().Status == "ready" && len(receiver.Received()) >= 2 })
	for partition := int32(0); partition < partitions; partition++ {
		produceRuntimeRecord(t, names.Topic, partition, fmt.Sprintf("revoked-partition-%d", partition), fmt.Sprintf("key-%d", partition), 10003+int64(partition))
	}
	waitRuntime(t, func() bool {
		for partition := int32(0); partition < partitions; partition++ {
			if countEventRequests(receiver.Received(), fmt.Sprintf("revoked-partition-%d", partition)) == 0 {
				return false
			}
		}
		return true
	})
	assigned := make(chan int32, 1)
	second := KafkaClient(t, kgo.ConsumerGroup(kafka.GroupID(controlScope(engine), app.Destinations[0].ID)), kgo.ConsumeTopics(names.Topic), kgo.DisableAutoCommit(), kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, ps map[string][]int32) {
		for _, partition := range ps[names.Topic] {
			select {
			case assigned <- partition:
			default:
			}
		}
	}))
	pollCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for pollCtx.Err() == nil {
			second.PollFetches(pollCtx)
		}
	}()
	var revoked int32
	select {
	case revoked = <-assigned:
	case <-time.After(30 * time.Second):
		t.Fatal("second consumer did not receive a partition")
	}
	eventID := fmt.Sprintf("revoked-partition-%d", revoked)
	before := countEventRequests(receiver.Received(), eventID)
	assertNoAdditionalRequests(t, receiver, eventID, before)
	if hasCommittedEvent(runtime, eventID) {
		t.Fatal("revoked retrying record was committed")
	}
	cancel()
	<-pollDone
	second.Close()
	allow.Store(true)
	waitRuntime(t, func() bool {
		for partition := int32(0); partition < partitions; partition++ {
			if !hasCommittedEvent(runtime, fmt.Sprintf("revoked-partition-%d", partition)) {
				return false
			}
		}
		return true
	})
}

func controlScope(engine config.Engine) (scope streams.Scope) {
	return streams.Scope{Namespace: engine.Namespace, Environment: engine.Environment, Generation: 1}
}

func committedCount(runtime *engineapp.App) int {
	page, _ := runtime.Logs("")
	count := 0
	for _, entry := range page.Entries {
		if entry.Message == "committed" {
			count++
		}
	}
	return count
}

func hasCommittedEvent(runtime *engineapp.App, eventID string) bool {
	page, _ := runtime.Logs("")
	for _, entry := range page.Entries {
		if entry.Message == "committed" && entry.EventID == eventID {
			return true
		}
	}
	return false
}

func hasLog(runtime *engineapp.App, message string) bool {
	page, _ := runtime.Logs("")
	for _, entry := range page.Entries {
		if entry.Message == message {
			return true
		}
	}
	return false
}

func countEventRequests(requests []ReceivedRequest, eventID string) int {
	count := 0
	for _, request := range requests {
		if request.Header.Get("X-Postie-Event-ID") == eventID {
			count++
		}
	}
	return count
}

func assertNoAdditionalRequests(t *testing.T, receiver *Receiver, eventID string, want int) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if got := countEventRequests(receiver.Received(), eventID); got != want {
			t.Fatalf("event %q received another HTTP attempt after ownership loss: before=%d after=%d", eventID, want, got)
		}
		select {
		case <-deadline.C:
			return
		case <-ticker.C:
		}
	}
}

func produceRuntimeRecord(t *testing.T, topic string, partition int32, eventID, correlationID string, id int64) {
	t.Helper()
	value, err := json.Marshal(map[string]any{
		"before": nil,
		"after": map[string]any{
			"id": id, "event_id": eventID, "aggregate_id": "aggregate-" + eventID,
			"aggregate_version": 1, "causation_id": eventID, "correlation_id": correlationID,
			"recorded_on": "2024-01-01T00:00:00Z", "event_name": "RuntimeTest",
			"payload": "{}", "json_metadata": "{}",
		},
		"source": map[string]string{"schema": "public", "table": topicTable(topic)},
		"op":     "c",
	})
	if err != nil {
		t.Fatal(err)
	}
	cl := KafkaClient(t, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	results := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: partition, Key: []byte(fmt.Sprintf(`{"correlation_id":%q}`, correlationID)), Value: value})
	if err := results.FirstErr(); err != nil {
		t.Fatalf("produce %s[%d]: %v", topic, partition, err)
	}
}

func topicTable(topic string) string {
	// Debezium appends .public.<table> to the generated topic prefix.
	parts := strings.Split(topic, ".")
	return parts[len(parts)-1]
}

func TestRuntimeMissingSlotBlocksEvenWhenConnectorIsUnavailable(t *testing.T) {
	engine, app, _, table := runtimeFixture(t)
	names := provisioning.NamesFor(namespace, environment, engineapp.Source(HostSource(table)), 1)
	runtime, stop := startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return runtime.Status().Status == "ready" })

	if err := Stop("connect"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := Start("connect"); err != nil {
			t.Error(err)
		}
	})
	dropSlot(t, names.Slot)

	waitRuntime(t, func() bool { return historySourceBlocked(runtime, table) })
	assertStoredBlock(t, table, "replication slot")
	stop()
}

func TestRuntimeRecreatedTopicUUIDBlocksEstablishedStream(t *testing.T) {
	engine, app, _, table := runtimeFixture(t)
	names := provisioning.NamesFor(namespace, environment, engineapp.Source(HostSource(table)), 1)
	runtime, stop := startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return runtime.Status().Status == "ready" })
	stop()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	adm := Admin(t)
	oldID := historyTopicID(t, adm, names.Topic)
	if _, err := adm.DeleteTopic(ctx, names.Topic); err != nil {
		t.Fatalf("delete established topic: %v", err)
	}
	waitTopicAbsent(t, adm, names.Topic)
	newID, err := (&kafka.Admin{Client: adm}).EnsureTopic(ctx, names, partitions, replication)
	if err != nil {
		t.Fatalf("recreate topic: %v", err)
	}
	if newID == oldID {
		t.Fatalf("recreated topic kept established UUID %x", oldID)
	}

	runtime, _ = startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return historySourceBlocked(runtime, table) })
	assertStoredBlock(t, table, "topic")
}

func TestRuntimeMissingCommittedOffsetBlocksStartedPartition(t *testing.T) {
	engine, app, _, table := runtimeFixture(t)
	names := provisioning.NamesFor(namespace, environment, engineapp.Source(HostSource(table)), 1)
	runtime, stop := startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return runtime.Status().Status == "ready" })
	stop()

	partition := historyBusyPartition(t, names.Topic)
	group := kafka.GroupID(streams.Scope{Namespace: namespace, Environment: environment, Generation: 1}, app.Destinations[0].ID)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var topics kadm.TopicsSet
	topics.Add(names.Topic, partition)
	if result, err := Admin(t).DeleteOffsets(ctx, group, topics); err != nil || result.Error() != nil {
		t.Fatalf("delete committed offset: result=%v err=%v", result, err)
	}

	runtime, _ = startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return historySourceBlocked(runtime, table) })
	assertStoredBlock(t, table, "offset")
}

func TestRuntimeOutOfRangeCommittedOffsetBlocksAfterRetention(t *testing.T) {
	engine, app, _, table := runtimeFixture(t)
	names := provisioning.NamesFor(namespace, environment, engineapp.Source(HostSource(table)), 1)
	runtime, stop := startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return runtime.Status().Status == "ready" })
	stop()

	partition := historyBusyPartition(t, names.Topic)
	group := kafka.GroupID(streams.Scope{Namespace: namespace, Environment: environment, Generation: 1}, app.Destinations[0].ID)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	adm := Admin(t)
	commits := kadm.Offsets{}
	commits.AddOffset(names.Topic, partition, 0, -1)
	if result, err := adm.CommitOffsets(ctx, group, commits); err != nil || result.Error() != nil {
		t.Fatalf("reset committed offset: result=%v err=%v", result, err)
	}
	deleteAt := kadm.Offsets{}
	deleteAt.AddOffset(names.Topic, partition, 1, -1)
	if result, err := adm.DeleteRecords(ctx, deleteAt); err != nil || result.Error() != nil {
		t.Fatalf("advance retention boundary: result=%v err=%v", result, err)
	}
	historyWaitStartOffset(t, adm, names.Topic, partition, 1)

	runtime, _ = startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return historySourceBlocked(runtime, table) })
	assertStoredBlock(t, table, "history")
}

func TestRuntimeAuditedSkipSurvivesBackwardOffsetReset(t *testing.T) {
	engine, app, receiver, table := runtimeFixture(t)
	receiver.SetReply(func(ReceivedRequest) (int, string) {
		return 200, `{"result":{"error":{"policy":"keep_going","class":"history","description":"audit"}}}`
	})
	names := provisioning.NamesFor(namespace, environment, engineapp.Source(HostSource(table)), 1)
	runtime, stop := startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return historyCommittedCount(runtime) == 2 })
	before := len(receiver.Received())
	stop()

	partition := historyBusyPartition(t, names.Topic)
	group := kafka.GroupID(streams.Scope{Namespace: namespace, Environment: environment, Generation: 1}, app.Destinations[0].ID)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	offsets := kadm.Offsets{}
	offsets.AddOffset(names.Topic, partition, 0, -1)
	if result, err := Admin(t).CommitOffsets(ctx, group, offsets); err != nil || result.Error() != nil {
		t.Fatalf("reset audited partition offset: result=%v err=%v", result, err)
	}

	runtime, _ = startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return historyCommittedCount(runtime) == 2 })
	if got := len(receiver.Received()); got != before {
		t.Fatalf("audited skip was redelivered after offset reset: before=%d after=%d", before, got)
	}
}

func dropSlot(t *testing.T, slot string) {
	t.Helper()
	conn := SourceDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, slot); err != nil {
		t.Fatalf("drop slot %q: %v", slot, err)
	}
}

func historySourceBlocked(runtime *engineapp.App, sourceID string) bool {
	for _, source := range runtime.Status().Sources {
		if source.ID == sourceID {
			return source.State == "blocked"
		}
	}
	return false
}

func historyCommittedCount(runtime *engineapp.App) int {
	page, _ := runtime.Logs("")
	count := 0
	for _, entry := range page.Entries {
		if entry.Message == "committed" {
			count++
		}
	}
	return count
}

func assertStoredBlock(t *testing.T, sourceID, want string) {
	t.Helper()
	store, err := controlpg.Open(context.Background(), controlConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stream, found, err := store.GetStream(context.Background(), streams.Scope{Namespace: namespace, Environment: environment, Generation: 1}, sourceID)
	if err != nil || !found {
		t.Fatalf("stored stream found=%v err=%v", found, err)
	}
	if !strings.Contains(stream.Blocked, want) {
		t.Fatalf("stored block reason %q does not contain %q", stream.Blocked, want)
	}
}

func historyTopicID(t *testing.T, adm *kadm.Client, topic string) [16]byte {
	t.Helper()
	details, err := adm.ListTopics(context.Background(), topic)
	if err != nil {
		t.Fatal(err)
	}
	detail, ok := details[topic]
	if !ok || detail.Err != nil {
		t.Fatalf("topic %q unavailable: %v", topic, detail.Err)
	}
	return [16]byte(detail.ID)
}

func waitTopicAbsent(t *testing.T, adm *kadm.Client, topic string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		details, err := adm.ListTopics(context.Background(), topic)
		if err == nil {
			detail, found := details[topic]
			if !found || detail.Err != nil {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("topic %q did not disappear", topic)
}

func historyBusyPartition(t *testing.T, topic string) int32 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	end, err := Admin(t).ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	for partition := int32(0); partition < partitions; partition++ {
		offset, ok := end.Lookup(topic, partition)
		if ok && offset.Offset > 0 {
			return partition
		}
	}
	t.Fatalf("topic %q has no partition with captured records", topic)
	return -1
}

func historyWaitStartOffset(t *testing.T, adm *kadm.Client, topic string, partition int32, want int64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		start, err := adm.ListStartOffsets(context.Background(), topic)
		if err == nil {
			if offset, ok := start.Lookup(topic, partition); ok && offset.Offset >= want {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("topic %q[%d] did not reach start offset %d", topic, partition, want)
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
	cmd := exec.CommandContext(ctx, "go", "run", "./cmd/postie", "provision", "--config", enginePath, "--generation", "1")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("postie provision unexpectedly succeeded: %s", out)
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
