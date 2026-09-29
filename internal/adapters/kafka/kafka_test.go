package kafka

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/stream"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
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

func TestValidateTopicReplication(t *testing.T) {
	for _, tc := range []struct {
		name       string
		detail     kadm.TopicDetail
		wantError  error
		wantReason string
	}{
		{
			name: "all partitions match even with a smaller ISR",
			detail: kadm.TopicDetail{Topic: "events", Partitions: kadm.PartitionDetails{
				0: {Partition: 0, Replicas: []int32{1, 2, 3}, ISR: []int32{1}},
				1: {Partition: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}},
			}},
		},
		{
			name: "later partition has weaker replication",
			detail: kadm.TopicDetail{Topic: "events", Partitions: kadm.PartitionDetails{
				0: {Partition: 0, Replicas: []int32{1, 2, 3}},
				1: {Partition: 1, Replicas: []int32{1}},
			}},
			wantReason: `topic "events" partition 1 has 1 replicas, want 3`,
		},
		{
			name: "excess replicas also differ from requested count",
			detail: kadm.TopicDetail{Topic: "events", Partitions: kadm.PartitionDetails{
				0: {Partition: 0, Replicas: []int32{1, 2, 3, 4}},
			}},
			wantReason: `topic "events" partition 0 has 4 replicas, want 3`,
		},
		{
			name:      "topic metadata error is preserved",
			detail:    kadm.TopicDetail{Topic: "events", Err: kerr.TopicAuthorizationFailed},
			wantError: kerr.TopicAuthorizationFailed,
		},
		{
			name: "later metadata error takes precedence over earlier mismatch",
			detail: kadm.TopicDetail{Topic: "events", Partitions: kadm.PartitionDetails{
				0: {Partition: 0, Replicas: []int32{1}},
				1: {Partition: 1, Err: kerr.LeaderNotAvailable},
			}},
			wantError: kerr.LeaderNotAvailable,
		},
		{
			name: "earlier metadata error takes precedence over later mismatch",
			detail: kadm.TopicDetail{Topic: "events", Partitions: kadm.PartitionDetails{
				0: {Partition: 0, Err: kerr.LeaderNotAvailable},
				1: {Partition: 1, Replicas: []int32{1}},
			}},
			wantError: kerr.LeaderNotAvailable,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTopicReplication(tc.detail, 3)
			switch {
			case tc.wantError != nil:
				if !errors.Is(err, tc.wantError) {
					t.Fatalf("got %v, want wrapped %v", err, tc.wantError)
				}
			case tc.wantReason != "":
				if err == nil || !strings.Contains(err.Error(), tc.wantReason) {
					t.Fatalf("got %v, want %q", err, tc.wantReason)
				}
			default:
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
