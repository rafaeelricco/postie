package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func (w *partitionWorker) initialize() error {
	c := w.consumer
	source := c.sources[w.key.topic]
	return retry(w.ctx, time.Second, func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		committed, err := c.admin.Client.FetchOffsets(ctx, c.group)
		if err != nil {
			return err
		}
		start, err := c.admin.Client.ListStartOffsets(ctx, w.key.topic)
		if err != nil {
			return err
		}
		end, err := c.admin.Client.ListEndOffsets(ctx, w.key.topic)
		if err != nil {
			return err
		}
		first, exists := start.Lookup(w.key.topic, w.key.partition)
		if !exists || first.Err != nil {
			return fmt.Errorf("partition start unavailable")
		}
		last, exists := end.Lookup(w.key.topic, w.key.partition)
		if !exists || last.Err != nil {
			return fmt.Errorf("partition end unavailable")
		}
		offset, exists := committed.Lookup(w.key.topic, w.key.partition)
		if exists && offset.Err != nil {
			return offset.Err
		}
		started, err := c.store.PartitionStarted(ctx, c.scope, c.destination, w.key.topic, w.key.partition)
		if err != nil {
			return err
		}
		if !exists || offset.At < 0 {
			if started || first.Offset != 0 {
				c.dispatch.Block(source.ID, "established consumer offsets or history lost")
				w.cancel()
				return context.Canceled
			}
			// Establish a durable baseline before marking this partition as started.
			if err := w.commit(ctx, &kgo.Record{Topic: w.key.topic, Partition: w.key.partition, Offset: first.Offset - 1, LeaderEpoch: -1}); err != nil {
				return err
			}
		} else if offset.At < first.Offset || offset.At > last.Offset {
			c.dispatch.Block(source.ID, "committed offset is outside retained history")
			w.cancel()
			return context.Canceled
		}
		return c.store.MarkPartitionStarted(ctx, c.scope, c.destination, w.key.topic, w.key.partition)
	})
}

func (w *partitionWorker) commit(ctx context.Context, record *kgo.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.owned || w.ctx.Err() != nil || !w.consumer.dispatch.Allowed(w.consumer.sources[w.key.topic].ID) {
		return context.Canceled
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return w.consumer.client.CommitRecords(ctx, record)
}

func retry(ctx context.Context, delay time.Duration, fn func(context.Context) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(ctx); err == nil {
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
