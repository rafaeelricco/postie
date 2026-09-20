package control

import (
	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/stream"
	"io"
	"testing"
	"time"
)

func TestDispatchRequiresCurrentLeaseAndHealthyDependencies(t *testing.T) {
	r := &Service{status: Status{Control: true, Kafka: true}, leaseUntil: time.Now().Add(time.Minute), sourceStates: map[string]SourceStatus{"events": {State: "running"}}}
	if !r.Allowed("events") {
		t.Fatal("healthy lease rejected")
	}
	r.leaseUntil = time.Now().Add(-time.Second)
	if r.Allowed("events") {
		t.Fatal("expired lease permits dispatch")
	}
	r.leaseUntil = time.Now().Add(time.Minute)
	r.status.Control = false
	if r.Allowed("events") {
		t.Fatal("failed control refresh permits dispatch")
	}
	r.status.Control = true
	r.status.Kafka = false
	if r.Allowed("events") {
		t.Fatal("failed Kafka health permits dispatch")
	}
	r.status.Kafka = true
	r.sourceStates["events"] = SourceStatus{State: "blocked"}
	if r.Allowed("events") {
		t.Fatal("blocked source permits dispatch")
	}
	if r.Allowed("unknown") {
		t.Fatal("unknown source permits dispatch")
	}
}

func TestSourceRefreshCannotClearLatchedBlock(t *testing.T) {
	r := &Service{log: activity.New(io.Discard), streams: map[string]stream.Registration{"events": {SourceID: "events", Blocked: "history lost"}}, sourceStates: map[string]SourceStatus{"events": {ID: "events", State: "blocked", Error: "history lost"}}}
	// An inspection may have loaded this snapshot before the worker blocked.
	got := r.cacheStream(stream.Registration{SourceID: "events"})
	if got.Blocked != "history lost" {
		t.Fatal("stale inspection erased the permanent block")
	}
	r.sourceState("events", "running", "")
	if got := r.Status().Sources[0]; got.State != "blocked" || got.Error != "history lost" {
		t.Fatalf("stale health result reopened source: %+v", got)
	}
}
