package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/stream"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"sort"
	"sync"
)

type partitionKey struct {
	topic     string
	partition int32
}
type Consumer struct {
	scope       stream.Scope
	destination string
	admin       *Admin
	store       PartitionStore
	dispatch    Dispatch
	log         *activity.Log
	process     func(context.Context, *stream.RawRecord, func(context.Context, *stream.RawRecord) error) error
	group       string
	client      *kgo.Client
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	mu          sync.Mutex
	assigned    bool
	workers     map[partitionKey]*partitionWorker
	sources     map[string]stream.Source
}
type partitionWorker struct {
	consumer    *Consumer
	key         partitionKey
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	batches     chan []*kgo.Record
	mu          sync.Mutex
	owned       bool
	initialized bool
}
type PartitionStore interface {
	PartitionStarted(context.Context, stream.Scope, string, string, int32) (bool, error)
	MarkPartitionStarted(context.Context, stream.Scope, string, string, int32) error
}
type Dispatch interface {
	Allowed(string) bool
	Block(string, string)
	Signal()
}
type ConsumerOptions struct {
	Brokers     []string
	Scope       stream.Scope
	Destination string
	Sources     map[string]stream.Source
	Admin       *Admin
	Store       PartitionStore
	Dispatch    Dispatch
	Log         *activity.Log
	Process     func(context.Context, *stream.RawRecord, func(context.Context, *stream.RawRecord) error) error
}

func GroupID(scope stream.Scope, destination string) string {
	return fmt.Sprintf("%s.%s.%s.g%s", scope.Namespace, scope.Environment, destination, scope.Generation.String())
}
func NewConsumer(ctx context.Context, options ConsumerOptions) (*Consumer, error) {
	ctx, cancel := context.WithCancel(ctx)
	c := &Consumer{scope: options.Scope, destination: options.Destination, admin: options.Admin, store: options.Store, dispatch: options.Dispatch, log: options.Log, process: options.Process, group: GroupID(options.Scope, options.Destination), ctx: ctx, cancel: cancel, done: make(chan struct{}), workers: map[partitionKey]*partitionWorker{}, sources: options.Sources}
	topics := make([]string, 0, len(options.Sources))
	for topic := range options.Sources {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	cl, err := kgo.NewClient(kgo.SeedBrokers(options.Brokers...), kgo.ConsumerGroup(c.group), kgo.ConsumeTopics(topics...), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()), kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.FetchMaxBytes(8<<20), kgo.FetchMaxPartitionBytes(5<<20), kgo.OnPartitionsAssigned(c.onAssigned), kgo.OnPartitionsRevoked(c.onRevoked), kgo.OnPartitionsLost(c.onLost))
	if err != nil {
		cancel()
		return nil, err
	}
	c.client = cl
	return c, nil
}
func (c *Consumer) Cancel() { c.cancel() }
func (c *Consumer) Ready() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.assigned {
		return false
	}
	for _, w := range c.workers {
		if !w.initialized {
			return false
		}
	}
	return true
}
func (c *Consumer) Finished() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}
func (c *Consumer) Stop() { c.cancel(); <-c.done }
func (c *Consumer) Run() {
	defer close(c.done)
	defer c.client.Close()
	defer c.stopWorkers(nil)
	defer c.client.AllowRebalance()
	for c.ctx.Err() == nil {
		fetches := c.client.PollRecords(c.ctx, 128)
		for _, failure := range fetches.Errors() {
			if c.ctx.Err() != nil {
				return
			}
			var lost *kgo.ErrDataLoss
			if errors.Is(failure.Err, kerr.OffsetOutOfRange) || errors.As(failure.Err, &lost) {
				c.dispatch.Block(c.sources[failure.Topic].ID, "Kafka history boundary lost")
				c.cancel()
				return
			}
			c.log.Add(activity.Entry{Level: "warn", Message: "Kafka fetch unavailable", Destination: c.destination, Topic: failure.Topic, Partition: failure.Partition, Error: "fetch failed"})
		}
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			key := partitionKey{p.Topic, p.Partition}
			c.mu.Lock()
			w := c.workers[key]
			c.mu.Unlock()
			if w == nil || w.ctx.Err() != nil {
				return
			} // revoked work is redelivered from its commit
			c.client.PauseFetchPartitions(map[string][]int32{p.Topic: {p.Partition}})
			select {
			case w.batches <- p.Records:
			case <-w.ctx.Done():
			case <-c.ctx.Done():
			}
		})
		// Fence ownership only while routing; HTTP retries must not delay rebalances.
		c.client.AllowRebalance()
	}
}
func (w *partitionWorker) run() {
	defer close(w.done)
	if err := w.initialize(); err != nil {
		if w.ctx.Err() == nil {
			w.consumer.cancel()
		}
		return
	}
	c := w.consumer
	c.mu.Lock()
	w.initialized = true
	c.mu.Unlock()
	c.dispatch.Signal()
	for {
		select {
		case <-w.ctx.Done():
			return
		case records := <-w.batches:
			for _, record := range records {
				if err := w.process(record); err != nil {
					if w.ctx.Err() == nil {
						c.cancel()
					}
					return
				}
			}
			c.mu.Lock()
			if c.workers[w.key] == w && w.ctx.Err() == nil {
				c.client.ResumeFetchPartitions(map[string][]int32{w.key.topic: {w.key.partition}})
			}
			c.mu.Unlock()
		}
	}
}

func (w *partitionWorker) process(raw *kgo.Record) error {
	record := &stream.RawRecord{Topic: raw.Topic, Partition: raw.Partition, Offset: raw.Offset, LeaderEpoch: raw.LeaderEpoch, Key: raw.Key, Value: raw.Value}
	return w.consumer.process(w.ctx, record, func(ctx context.Context, position *stream.RawRecord) error {
		return w.commit(ctx, &kgo.Record{Topic: position.Topic, Partition: position.Partition, Offset: position.Offset, LeaderEpoch: position.LeaderEpoch})
	})
}
