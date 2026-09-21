package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/rafaeelricco/postie/internal/activity"
)

func (c *Consumer) onAssigned(ctx context.Context, cl *kgo.Client, partitions map[string][]int32) {
	c.mu.Lock()
	c.assigned = true
	for topic, ps := range partitions {
		for _, partition := range ps {
			key := partitionKey{topic, partition}
			if _, exists := c.workers[key]; exists {
				continue
			}
			workerCtx, cancel := context.WithCancel(c.ctx)
			w := &partitionWorker{consumer: c, key: key, ctx: workerCtx, cancel: cancel, done: make(chan struct{}), batches: make(chan []*kgo.Record, 1), owned: true}
			c.workers[key] = w
			go w.run()
		}
	}
	c.mu.Unlock()
	cl.ResumeFetchPartitions(partitions)
	c.log.Add(activity.Entry{Message: "partitions assigned", Destination: c.destination})
}

func (c *Consumer) onRevoked(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
	c.stopWorkers(partitions)
	c.log.Add(activity.Entry{Message: "partitions revoked", Destination: c.destination})
}

func (c *Consumer) onLost(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
	c.mu.Lock()
	c.assigned = false
	c.mu.Unlock()
	c.stopWorkers(partitions)
	c.log.Add(activity.Entry{Level: "warn", Message: "partition ownership lost", Destination: c.destination})
	c.dispatch.Signal()
}

func (c *Consumer) stopWorkers(partitions map[string][]int32) {
	c.mu.Lock()
	workers := []*partitionWorker{}
	for key, w := range c.workers {
		remove := partitions == nil
		for _, partition := range partitions[key.topic] {
			if partition == key.partition {
				remove = true
			}
		}
		if remove {
			w.cancel()
			workers = append(workers, w)
			delete(c.workers, key)
		}
	}
	c.mu.Unlock()
	for _, w := range workers {
		w.mu.Lock()
		w.owned = false
		w.mu.Unlock()
		<-w.done
	}
}
