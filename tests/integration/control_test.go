//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rafaeelricco/postie/internal/adapters/controlpg"
	engineapp "github.com/rafaeelricco/postie/internal/app"
	"github.com/rafaeelricco/postie/internal/config"
	streams "github.com/rafaeelricco/postie/internal/stream"
)

func TestControlStorePersistsSafeRestartState(t *testing.T) {
	ctx := context.Background()
	store, err := controlpg.Open(ctx, controlConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	scope := streams.Scope{Namespace: UniqueNamespace(t), Environment: "integration", Generation: 1}
	stream := streams.Registration{
		SourceID: "source",
		Identity: streams.Identity{Table: "events", Partitions: 3, Columns: []streams.Column{{Name: "id", Type: streams.PGInt8}}},
		Names:    streams.Names{Topic: "topic", Connector: "connector", Slot: "slot"},
		TopicID:  [16]byte{1, 2, 3},
	}
	if err := store.RegisterStream(ctx, scope, stream); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterStream(ctx, scope, stream); err != nil {
		t.Fatalf("idempotent registration: %v", err)
	}
	got, found, err := store.GetStream(ctx, scope, stream.SourceID)
	if err != nil || !found || got.TopicID != stream.TopicID {
		t.Fatalf("GetStream = %#v, %v, %v", got, found, err)
	}

	if err := store.EnsureSubscriptions(ctx, scope, []string{"destination"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetDesired(ctx, scope, "destination", "paused"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetDesired(ctx, scope, "destination", "paused"); err != nil {
		t.Fatal(err)
	}
	if err := store.RenewLease(ctx, scope, "worker", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	record := streams.Record{Source: engineapp.Source(config.Source{ID: "source", Password: "secret"}), Topic: "topic", Partition: 1, Offset: 42, Payload: []byte(`{"event":"skipped"}`)}
	if err := store.SaveSkip(ctx, scope, "destination", record); err != nil {
		t.Fatal(err)
	}
	if ok, err := store.HasSkip(ctx, scope, "destination", record); err != nil || !ok {
		t.Fatalf("HasSkip = %v, %v", ok, err)
	}
	started, err := store.PartitionStarted(ctx, scope, "destination", "topic", 1)
	if err != nil || started {
		t.Fatalf("new partition marker = %v, %v", started, err)
	}
	if err := store.MarkPartitionStarted(ctx, scope, "destination", "topic", 1); err != nil {
		t.Fatal(err)
	}
	started, err = store.PartitionStarted(ctx, scope, "destination", "topic", 1)
	if err != nil || !started {
		t.Fatalf("marked partition = %v, %v", started, err)
	}
	if err := store.BlockSource(ctx, scope, stream.SourceID, "topic history lost"); err != nil {
		t.Fatal(err)
	}

	conn, err := pgx.Connect(ctx, controlConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var raw string
	if err := conn.QueryRow(ctx, `SELECT identity::text || names::text FROM postie_streams WHERE namespace=$1`, scope.Namespace).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(raw, "secret") {
		t.Fatalf("control metadata contains a secret: %s", raw)
	}
}

func TestControlSubscriptionObservedWaitsForEveryWorker(t *testing.T) {
	ctx := context.Background()
	store, err := controlpg.Open(ctx, controlConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	scope := streams.Scope{Namespace: UniqueNamespace(t), Environment: "integration", Generation: 1}
	if err := store.EnsureSubscriptions(ctx, scope, []string{"destination"}); err != nil {
		t.Fatal(err)
	}
	desired, err := store.SetDesired(ctx, scope, "destination", "paused")
	if err != nil {
		t.Fatal(err)
	}
	for _, worker := range []string{"worker-a", "worker-b"} {
		if err := store.RenewLease(ctx, scope, worker, 30*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.ObserveSubscription(ctx, scope, "worker-a", "destination", desired.Revision, "paused"); err != nil {
		t.Fatal(err)
	}
	if observed, err := store.SubscriptionObserved(ctx, scope, "destination", desired.Revision, "paused"); err != nil || observed {
		t.Fatalf("one worker observed = %v, %v; expected pending", observed, err)
	}
	if err := store.ObserveSubscription(ctx, scope, "worker-b", "destination", desired.Revision, "running"); err != nil {
		t.Fatal(err)
	}
	if observed, err := store.SubscriptionObserved(ctx, scope, "destination", desired.Revision, "paused"); err != nil || observed {
		t.Fatalf("mismatching worker observed = %v, %v; expected pending", observed, err)
	}
	if err := store.ObserveSubscription(ctx, scope, "worker-b", "destination", desired.Revision, "paused"); err != nil {
		t.Fatal(err)
	}
	if observed, err := store.SubscriptionObserved(ctx, scope, "destination", desired.Revision, "paused"); err != nil || !observed {
		t.Fatalf("both workers observed = %v, %v; expected complete", observed, err)
	}
}

func TestControlSubscriptionObservedExcludesExpiredWorker(t *testing.T) {
	ctx := context.Background()
	store, err := controlpg.Open(ctx, controlConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	scope := streams.Scope{Namespace: UniqueNamespace(t), Environment: "integration", Generation: 1}
	if err := store.EnsureSubscriptions(ctx, scope, []string{"destination"}); err != nil {
		t.Fatal(err)
	}
	for _, worker := range []string{"active", "expired"} {
		ttl := 30 * time.Second
		if worker == "expired" {
			ttl = 50 * time.Millisecond
		}
		if err := store.RenewLease(ctx, scope, worker, ttl); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.ObserveSubscription(ctx, scope, "active", "destination", 1, "paused"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if observed, err := store.SubscriptionObserved(ctx, scope, "destination", 1, "paused"); err != nil || !observed {
		t.Fatalf("expired worker remains blocking: observed=%v err=%v", observed, err)
	}
}

func TestControlSubscriptionObservedRequiresMatchingRevisionAndState(t *testing.T) {
	ctx := context.Background()
	store, err := controlpg.Open(ctx, controlConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	scope := streams.Scope{Namespace: UniqueNamespace(t), Environment: "integration", Generation: 1}
	if err := store.EnsureSubscriptions(ctx, scope, []string{"destination"}); err != nil {
		t.Fatal(err)
	}
	if err := store.RenewLease(ctx, scope, "worker", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	first, err := store.SetDesired(ctx, scope, "destination", "running")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ObserveSubscription(ctx, scope, "worker", "destination", first.Revision, "running"); err != nil {
		t.Fatal(err)
	}
	if observed, err := store.SubscriptionObserved(ctx, scope, "destination", first.Revision, "running"); err != nil || !observed {
		t.Fatalf("initial observation = %v, %v; expected complete", observed, err)
	}
	second, err := store.SetDesired(ctx, scope, "destination", "paused")
	if err != nil {
		t.Fatal(err)
	}
	if second.Revision <= first.Revision {
		t.Fatalf("revision did not advance: first=%d second=%d", first.Revision, second.Revision)
	}
	if observed, err := store.SubscriptionObserved(ctx, scope, "destination", second.Revision, "paused"); err != nil || observed {
		t.Fatalf("new revision without observation = %v, %v; expected pending", observed, err)
	}
	if err := store.ObserveSubscription(ctx, scope, "worker", "destination", second.Revision, "running"); err != nil {
		t.Fatal(err)
	}
	if observed, err := store.SubscriptionObserved(ctx, scope, "destination", second.Revision, "paused"); err != nil || observed {
		t.Fatalf("mismatching state = %v, %v; expected pending", observed, err)
	}
	if err := store.ObserveSubscription(ctx, scope, "worker", "destination", second.Revision, "paused"); err != nil {
		t.Fatal(err)
	}
	if observed, err := store.SubscriptionObserved(ctx, scope, "destination", second.Revision, "paused"); err != nil || !observed {
		t.Fatalf("matching new revision = %v, %v; expected complete", observed, err)
	}
}

func TestControlReleaseLeaseExcludesWorkerAndObservations(t *testing.T) {
	ctx := context.Background()
	store, err := controlpg.Open(ctx, controlConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	scope := streams.Scope{Namespace: UniqueNamespace(t), Environment: "integration", Generation: 1}
	if err := store.EnsureSubscriptions(ctx, scope, []string{"destination"}); err != nil {
		t.Fatal(err)
	}
	for _, worker := range []string{"kept", "released"} {
		if err := store.RenewLease(ctx, scope, worker, 30*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.ObserveSubscription(ctx, scope, "kept", "destination", 1, "paused"); err != nil {
		t.Fatal(err)
	}
	if observed, err := store.SubscriptionObserved(ctx, scope, "destination", 1, "paused"); err != nil || observed {
		t.Fatalf("released worker before release = %v, %v; expected pending", observed, err)
	}
	if err := store.ReleaseLease(ctx, scope, "released"); err != nil {
		t.Fatal(err)
	}
	if observed, err := store.SubscriptionObserved(ctx, scope, "destination", 1, "paused"); err != nil || !observed {
		t.Fatalf("released worker after release = %v, %v; expected complete", observed, err)
	}
	if err := store.RenewLease(ctx, scope, "released", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if observed, err := store.SubscriptionObserved(ctx, scope, "destination", 1, "paused"); err != nil || observed {
		t.Fatalf("released worker observation was retained = %v, %v; expected pending", observed, err)
	}
}
