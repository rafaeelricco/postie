//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/adapters/kafka"
	engineapp "github.com/rafaeelricco/postie/internal/app"
	"github.com/rafaeelricco/postie/internal/config"
	provisioning "github.com/rafaeelricco/postie/internal/provision"
	streams "github.com/rafaeelricco/postie/internal/stream"
	"github.com/twmb/franz-go/pkg/kgo"
)

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
