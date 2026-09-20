//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/control"

	"github.com/rafaeelricco/postie/internal/adapters/controlpg"
	engineapp "github.com/rafaeelricco/postie/internal/app"
	"github.com/rafaeelricco/postie/internal/config"
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
