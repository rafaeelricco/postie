package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/rafaeelricco/postie/internal/provision"
	"github.com/rafaeelricco/postie/internal/stream"
)

// Admin is the Kafka administration adapter.
type Admin struct {
	Client *kadm.Client
	client *kgo.Client
}

func NewAdmin(brokers []string) (*Admin, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	return &Admin{Client: kadm.NewClient(client), client: client}, nil
}

func (a *Admin) Ping(ctx context.Context) error {
	if a.client != nil {
		return a.client.Ping(ctx)
	}
	_, err := a.Client.BrokerMetadata(ctx)
	return err
}

func (a *Admin) Close() {
	if a != nil && a.Client != nil {
		a.Client.Close()
	}
}

func topicConfigs(replication int16) map[string]*string {
	configs := map[string]*string{
		"cleanup.policy":  kadm.StringPtr("delete"),
		"retention.ms":    kadm.StringPtr("-1"),
		"retention.bytes": kadm.StringPtr("-1"),
	}
	if replication >= 3 {
		configs["min.insync.replicas"] = kadm.StringPtr("2")
	}
	return configs
}

// ValidateTopicReplication checks replica assignments, not current ISR size.
// Metadata errors take precedence over replica-count mismatches.
func ValidateTopicReplication(detail kadm.TopicDetail, replication int16) error {
	if detail.Err != nil {
		return fmt.Errorf("capture: inspect topic %q: %w", detail.Topic, detail.Err)
	}
	partitions := detail.Partitions.Sorted()
	for _, partition := range partitions {
		if partition.Err != nil {
			return fmt.Errorf("capture: inspect topic %q partition %d: %w", detail.Topic, partition.Partition, partition.Err)
		}
	}
	for _, partition := range partitions {
		if len(partition.Replicas) != int(replication) {
			return fmt.Errorf("capture: topic %q partition %d has %d replicas, want %d", detail.Topic, partition.Partition, len(partition.Replicas), replication)
		}
	}
	return nil
}

func (a *Admin) EnsureTopic(ctx context.Context, n stream.Names, partitions int32, replication int16) ([16]byte, error) {
	wantConfigs := topicConfigs(replication)
	id, exists, err := a.existingTopic(ctx, n.Topic, partitions, replication, wantConfigs)
	if err != nil || exists {
		return id, err
	}
	resp, err := a.Client.CreateTopic(ctx, partitions, replication, wantConfigs, n.Topic)
	if err == nil {
		err = resp.Err
	}
	if err == nil {
		return [16]byte(resp.ID), nil
	}
	if !errors.Is(err, kerr.TopicAlreadyExists) {
		return [16]byte{}, fmt.Errorf("capture: create topic %q: %w", n.Topic, err)
	}
	for {
		id, exists, err := a.existingTopic(ctx, n.Topic, partitions, replication, wantConfigs)
		if err != nil || exists {
			return id, err
		}
		select {
		case <-ctx.Done():
			return [16]byte{}, fmt.Errorf("capture: topic %q exists but never appeared in metadata: %w", n.Topic, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (a *Admin) existingTopic(ctx context.Context, topic string, partitions int32, replication int16, want map[string]*string) ([16]byte, bool, error) {
	listed, err := a.Client.ListTopics(ctx, topic)
	if err != nil {
		return [16]byte{}, false, fmt.Errorf("capture: list topic %q: %w", topic, err)
	}
	detail, ok := listed[topic]
	if !ok || detail.Err != nil {
		return [16]byte{}, false, nil
	}
	if len(detail.Partitions) != int(partitions) {
		return [16]byte{}, true, fmt.Errorf("capture: topic %q already exists with %d partitions, want %d", topic, len(detail.Partitions), partitions)
	}
	if err := ValidateTopicReplication(detail, replication); err != nil {
		return [16]byte{}, true, err
	}
	if err := a.verifyTopicConfigs(ctx, topic, want); err != nil {
		return [16]byte{}, true, err
	}
	return [16]byte(detail.ID), true, nil
}

func (a *Admin) verifyTopicConfigs(ctx context.Context, topic string, want map[string]*string) error {
	resourceConfigs, err := a.Client.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return fmt.Errorf("capture: describe topic %q configs: %w", topic, err)
	}
	rc, err := resourceConfigs.On(topic, nil)
	if err != nil {
		return fmt.Errorf("capture: describe topic %q configs: %w", topic, err)
	}
	if rc.Err != nil {
		return fmt.Errorf("capture: describe topic %q configs: %w", topic, rc.Err)
	}
	current := make(map[string]string, len(rc.Configs))
	for _, c := range rc.Configs {
		current[c.Key] = c.MaybeValue()
	}
	for key, value := range want {
		if value != nil && current[key] != *value {
			return fmt.Errorf("capture: topic %q config %q is %q, want %q", topic, key, current[key], *value)
		}
	}
	return nil
}

func (a *Admin) Topic(ctx context.Context, name string) (provision.TopicFacts, error) {
	listed, err := a.Client.ListTopics(ctx, name)
	if err != nil {
		return provision.TopicFacts{}, fmt.Errorf("capture: list topic %q: %w", name, err)
	}
	detail, ok := listed[name]
	if !ok || errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
		return provision.TopicFacts{Exists: false}, nil
	}
	if detail.Err != nil {
		return provision.TopicFacts{}, &provision.TopicMetadataError{Cause: fmt.Errorf("capture: inspect topic %q: %w", name, detail.Err)}
	}
	for _, partition := range detail.Partitions.Sorted() {
		if partition.Err != nil {
			return provision.TopicFacts{}, &provision.TopicMetadataError{Partition: true, Cause: fmt.Errorf("capture: inspect topic %q partition %d: %w", name, partition.Partition, partition.Err)}
		}
	}
	facts := provision.TopicFacts{Exists: true, ID: [16]byte(detail.ID), Partitions: len(detail.Partitions), Replicas: map[int32]int{}}
	for partition, value := range detail.Partitions {
		facts.Replicas[partition] = len(value.Replicas)
	}
	return facts, nil
}
