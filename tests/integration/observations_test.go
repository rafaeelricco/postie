//go:build integration

package integration

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rafaeelricco/postie/internal/adapters/controlpg"
	"github.com/rafaeelricco/postie/internal/stream"
)

func TestControlNewWorkerMustObserveAndReleaseRemovesObservation(t *testing.T) {
	ctx := context.Background()
	store, err := controlpg.Open(ctx, controlConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	scope := stream.Scope{Namespace: UniqueNamespace(t), Environment: "integration", Generation: 1}
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
			if observed, err := store.SubscriptionObserved(ctx, scope, "destination", desired.Revision, "paused"); err != nil || observed {
				t.Fatalf("new live worker must hold convergence: observed=%v err=%v", observed, err)
			}
		}
		if err := store.ObserveSubscription(ctx, scope, worker, "destination", desired.Revision, "paused"); err != nil {
			t.Fatal(err)
		}
		if observed, err := store.SubscriptionObserved(ctx, scope, "destination", desired.Revision, "paused"); err != nil || !observed {
			t.Fatalf("all live workers observed: observed=%v err=%v", observed, err)
		}
	}
	// An older update cannot overwrite the acknowledged revision.
	if err := store.ObserveSubscription(ctx, scope, "joined", "destination", desired.Revision-1, "running"); err != nil {
		t.Fatal(err)
	}
	if observed, err := store.SubscriptionObserved(ctx, scope, "destination", desired.Revision, "paused"); err != nil || !observed {
		t.Fatalf("stale observation replaced current state: observed=%v err=%v", observed, err)
	}
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
	if observed, err := store.SubscriptionObserved(ctx, scope, "destination", desired.Revision, "paused"); err != nil || observed {
		t.Fatalf("rejoined worker reused a released observation: observed=%v err=%v", observed, err)
	}
}

func TestControlReopenPreservesRegistrationAndDesiredState(t *testing.T) {
	ctx := context.Background()
	store, err := controlpg.Open(ctx, controlConnString())
	if err != nil {
		t.Fatal(err)
	}
	scope := stream.Scope{Namespace: UniqueNamespace(t), Environment: "integration", Generation: 2}
	registered := stream.Registration{SourceID: "events", Identity: stream.Identity{Table: "events", Partitions: 2, Columns: []stream.Column{{Name: "id", Type: stream.PGInt8}}}, Names: stream.Names{Topic: "existing-topic", Connector: "existing-connector", Slot: "existing-slot", Publication: "existing-publication"}, TopicID: [16]byte{1, 9}, Blocked: "history lost"}
	if err := store.RegisterStream(ctx, scope, registered); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err := store.EnsureSubscriptions(ctx, scope, []string{"destination"}); err != nil {
		store.Close()
		t.Fatal(err)
	}
	desired, err := store.SetDesired(ctx, scope, "destination", "paused")
	if err != nil {
		store.Close()
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
