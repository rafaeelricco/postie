package kafka_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/rafaeelricco/postie/internal/adapters/kafka"
)

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
			err := kafka.ValidateTopicReplication(tc.detail, 3)
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

func TestValidateTopicReplicationRecoversWhenAssignmentsMatch(t *testing.T) {
	detail := kadm.TopicDetail{Topic: "events", Partitions: kadm.PartitionDetails{
		0: {Partition: 0, Replicas: []int32{1}},
	}}
	if err := kafka.ValidateTopicReplication(detail, 3); err == nil {
		t.Fatal("mismatching assignments accepted")
	}
	detail.Partitions[0] = kadm.PartitionDetail{Partition: 0, Replicas: []int32{1, 2, 3}}
	if err := kafka.ValidateTopicReplication(detail, 3); err != nil {
		t.Fatalf("repaired assignments rejected: %v", err)
	}
}
