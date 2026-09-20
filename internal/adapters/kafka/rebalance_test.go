package kafka

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/stream"
	"github.com/twmb/franz-go/pkg/kgo"
)

type testDispatch struct {
	allowed bool
	signals int
}

func (d *testDispatch) Allowed(string) bool  { return d.allowed }
func (d *testDispatch) Block(string, string) {}
func (d *testDispatch) Signal()              { d.signals++ }

func TestLostGroupOwnershipIsNotReady(t *testing.T) {
	dispatch := &testDispatch{}
	c := &Consumer{assigned: true, workers: map[partitionKey]*partitionWorker{}, log: activity.New(io.Discard), dispatch: dispatch}
	// An assigned member with zero partitions is healthy; a lost group member is not.
	if !c.Ready() {
		t.Fatal("joined empty assignment rejected")
	}
	c.onLost(context.Background(), nil, nil)
	if c.Ready() {
		t.Fatal("lost ownership reported ready with no workers")
	}
	if dispatch.signals != 1 {
		t.Fatalf("ownership loss did not signal reconciliation: %d", dispatch.signals)
	}
}

func TestLateAcknowledgementAfterRevocationCannotCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dispatch := &testDispatch{allowed: true}
	key := partitionKey{"events", 2}
	c := &Consumer{dispatch: dispatch, log: activity.New(io.Discard), sources: map[string]stream.Source{"events": {ID: "source"}}, workers: map[partitionKey]*partitionWorker{}}
	w := &partitionWorker{consumer: c, key: key, ctx: ctx, cancel: cancel, done: make(chan struct{}), owned: true}
	c.workers[key] = w
	entered := make(chan struct{})
	acknowledge := make(chan struct{})
	result := make(chan error, 1)
	c.process = func(ctx context.Context, record *stream.RawRecord, commit func(context.Context, *stream.RawRecord) error) error {
		if record.Topic != "events" || record.Partition != 2 || record.Offset != 41 || record.LeaderEpoch != 7 || string(record.Key) != "key" || string(record.Value) != "value" {
			t.Errorf("record metadata changed: %+v", record)
		}
		close(entered)
		<-acknowledge // Model a transport returning an acknowledgement after cancellation.
		return commit(context.Background(), record)
	}
	go func() {
		defer close(w.done)
		result <- w.process(&kgo.Record{Topic: "events", Partition: 2, Offset: 41, LeaderEpoch: 7, Key: []byte("key"), Value: []byte("value")})
	}()
	<-entered
	revoked := make(chan struct{})
	go func() {
		c.onRevoked(context.Background(), nil, map[string][]int32{"events": {2}})
		close(revoked)
	}()
	<-ctx.Done()
	close(acknowledge)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("late acknowledgement was not rejected: %v", err)
	}
	<-revoked
	if len(c.workers) != 0 || w.owned {
		t.Fatal("revoked partition retained ownership")
	}
}

func TestCommitRequiresOwnershipAndDispatch(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		owned, allowed, canceled bool
	}{
		{"revoked", false, true, false},
		{"dependency unavailable", true, false, false},
		{"canceled", true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			c := &Consumer{dispatch: &testDispatch{allowed: tc.allowed}, sources: map[string]stream.Source{"events": {ID: "source"}}}
			w := &partitionWorker{consumer: c, key: partitionKey{"events", 2}, ctx: ctx, owned: tc.owned}
			// No Kafka client: reaching the external commit would panic.
			if err := w.commit(context.Background(), &kgo.Record{}); !errors.Is(err, context.Canceled) {
				t.Fatalf("unsafe commit accepted: %v", err)
			}
		})
	}
}
