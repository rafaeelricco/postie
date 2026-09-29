//go:build integration

package integration

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rafaeelricco/postie/internal/adapters/controlpg"
	engineapp "github.com/rafaeelricco/postie/internal/app"
	"github.com/rafaeelricco/postie/internal/config"
	streams "github.com/rafaeelricco/postie/internal/stream"
)

// newStore opens the control store, closes it when the test ends, and returns
// it with a scope in a namespace no other test shares.
func newStore(t *testing.T) (*controlpg.Store, streams.Scope) {
	t.Helper()
	store, err := controlpg.Open(context.Background(), controlConnString())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return store, streams.Scope{Namespace: UniqueNamespace(t), Environment: "integration", Generation: 1}
}

// assertObserved fails the test unless SubscriptionObserved reports want for
// the destination's revision and state.
func assertObserved(t *testing.T, store *controlpg.Store, scope streams.Scope, id string, revision int64, state string, want bool) {
	t.Helper()
	observed, err := store.SubscriptionObserved(context.Background(), scope, id, revision, state)
	if err != nil || observed != want {
		t.Fatalf("SubscriptionObserved(%q, revision %d, %q) = %v, %v; want %v", id, revision, state, observed, err, want)
	}
}

func TestControlStorePersistsSafeRestartState(t *testing.T) {
	ctx := context.Background()
	store, scope := newStore(t)
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
	store, scope := newStore(t)
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
	assertObserved(t, store, scope, "destination", desired.Revision, "paused", false) // one worker observed; expected pending
	if err := store.ObserveSubscription(ctx, scope, "worker-b", "destination", desired.Revision, "running"); err != nil {
		t.Fatal(err)
	}
	assertObserved(t, store, scope, "destination", desired.Revision, "paused", false) // mismatching worker; expected pending
	if err := store.ObserveSubscription(ctx, scope, "worker-b", "destination", desired.Revision, "paused"); err != nil {
		t.Fatal(err)
	}
	assertObserved(t, store, scope, "destination", desired.Revision, "paused", true) // both workers observed; expected complete
}

func TestControlSubscriptionObservedExcludesExpiredWorker(t *testing.T) {
	ctx := context.Background()
	store, scope := newStore(t)
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
	assertObserved(t, store, scope, "destination", 1, "paused", true) // an expired worker must not keep blocking
}

func TestControlSubscriptionObservedRequiresMatchingRevisionAndState(t *testing.T) {
	ctx := context.Background()
	store, scope := newStore(t)
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
	assertObserved(t, store, scope, "destination", first.Revision, "running", true) // initial observation; expected complete
	second, err := store.SetDesired(ctx, scope, "destination", "paused")
	if err != nil {
		t.Fatal(err)
	}
	if second.Revision <= first.Revision {
		t.Fatalf("revision did not advance: first=%d second=%d", first.Revision, second.Revision)
	}
	assertObserved(t, store, scope, "destination", second.Revision, "paused", false) // new revision without observation; expected pending
	if err := store.ObserveSubscription(ctx, scope, "worker", "destination", second.Revision, "running"); err != nil {
		t.Fatal(err)
	}
	assertObserved(t, store, scope, "destination", second.Revision, "paused", false) // mismatching state; expected pending
	if err := store.ObserveSubscription(ctx, scope, "worker", "destination", second.Revision, "paused"); err != nil {
		t.Fatal(err)
	}
	assertObserved(t, store, scope, "destination", second.Revision, "paused", true) // matching new revision; expected complete
}

func TestControlReleaseLeaseExcludesWorkerAndObservations(t *testing.T) {
	ctx := context.Background()
	store, scope := newStore(t)
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
	assertObserved(t, store, scope, "destination", 1, "paused", false) // released worker before release; expected pending
	if err := store.ReleaseLease(ctx, scope, "released"); err != nil {
		t.Fatal(err)
	}
	assertObserved(t, store, scope, "destination", 1, "paused", true) // released worker after release; expected complete
	if err := store.RenewLease(ctx, scope, "released", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	assertObserved(t, store, scope, "destination", 1, "paused", false) // released worker observation was retained; expected pending
}

func TestControlNewWorkerMustObserveAndReleaseRemovesObservation(t *testing.T) {
	ctx := context.Background()
	store, scope := newStore(t)
	if err := store.EnsureSubscriptions(ctx, scope, []string{"destination"}); err != nil {
		t.Fatal(err)
	}
	desired, err := store.SetDesired(ctx, scope, "destination", "paused")
	if err != nil {
		t.Fatal(err)
	}
	for _, worker := range []string{"original", "joined"} {
		if err := store.RenewLease(ctx, scope, worker, time.Minute); err != nil {
			t.Fatal(err)
		}
		if worker == "joined" {
			assertObserved(t, store, scope, "destination", desired.Revision, "paused", false) // new live worker must hold convergence
		}
		if err := store.ObserveSubscription(ctx, scope, worker, "destination", desired.Revision, "paused"); err != nil {
			t.Fatal(err)
		}
		assertObserved(t, store, scope, "destination", desired.Revision, "paused", true) // all live workers observed
	}
	// An older update cannot overwrite the acknowledged revision.
	if err := store.ObserveSubscription(ctx, scope, "joined", "destination", desired.Revision-1, "running"); err != nil {
		t.Fatal(err)
	}
	assertObserved(t, store, scope, "destination", desired.Revision, "paused", true) // a stale observation must not replace the current state
	if err := store.ReleaseLease(ctx, scope, "joined"); err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, controlConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var rows int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM postie_worker_subscriptions WHERE namespace=$1 AND worker_id='joined'`, scope.Namespace).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("release retained %d observations", rows)
	}
	if err := store.RenewLease(ctx, scope, "joined", time.Minute); err != nil {
		t.Fatal(err)
	}
	assertObserved(t, store, scope, "destination", desired.Revision, "paused", false) // a rejoined worker must not reuse a released observation
}

func TestControlReopenPreservesRegistrationAndDesiredState(t *testing.T) {
	ctx := context.Background()
	store, scope := newStore(t)
	scope.Generation = 2
	registered := streams.Registration{SourceID: "events", Identity: streams.Identity{Table: "events", Partitions: 2, Columns: []streams.Column{{Name: "id", Type: streams.PGInt8}}}, Names: streams.Names{Topic: "existing-topic", Connector: "existing-connector", Slot: "existing-slot", Publication: "existing-publication"}, TopicID: [16]byte{1, 9}, Blocked: "history lost"}
	if err := store.RegisterStream(ctx, scope, registered); err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureSubscriptions(ctx, scope, []string{"destination"}); err != nil {
		t.Fatal(err)
	}
	desired, err := store.SetDesired(ctx, scope, "destination", "paused")
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	store, err = controlpg.Open(ctx, controlConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	got, found, err := store.GetStream(ctx, scope, "events")
	if err != nil || !found || !reflect.DeepEqual(got, registered) {
		t.Fatalf("registration after reopen = %+v, %v, %v; want %+v", got, found, err, registered)
	}
	subscriptions, err := store.Subscriptions(ctx, scope)
	if err != nil || len(subscriptions) != 1 || subscriptions[0] != desired {
		t.Fatalf("subscriptions after reopen = %+v, %v; want %+v", subscriptions, err, desired)
	}
}
