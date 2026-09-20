package controlpg

import (
	"context"
	"errors"
	"fmt"
	streams "github.com/rafaeelricco/postie/internal/stream"
)

func (s *Store) SaveSkip(ctx context.Context, scope streams.Scope, destination string, record streams.Record) error {
	if err := validScope(scope); err != nil {
		return err
	}
	if destination == "" || record.Topic == "" || record.Partition < 0 || record.Offset < 0 {
		return errors.New("control: destination, topic, non-negative partition, and offset are required")
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO postie_skips
		(namespace, environment, generation, destination_id, source_id, event_id, topic, partition, offset_value)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING`,
		scope.Namespace, scope.Environment, int64(scope.Generation), destination, record.Source.ID,
		record.EventID, record.Topic, record.Partition, record.Offset)
	if err != nil {
		return fmt.Errorf("control: save skip: %w", err)
	}
	return nil
}

// HasSkip reports whether the terminal skip for this destination and Kafka
// position has already been recorded. It is used before a retry after a
// process crash so a terminal destination acknowledgement is not repeated.

func (s *Store) HasSkip(ctx context.Context, scope streams.Scope, destination string, record streams.Record) (bool, error) {
	if err := validScope(scope); err != nil {
		return false, err
	}
	if destination == "" || record.Topic == "" || record.Partition < 0 || record.Offset < 0 {
		return false, errors.New("control: destination, topic, non-negative partition, and offset are required")
	}
	var found bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM postie_skips
		WHERE namespace=$1 AND environment=$2 AND generation=$3 AND destination_id=$4
		AND topic=$5 AND partition=$6 AND offset_value=$7)`, scope.Namespace, scope.Environment,
		int64(scope.Generation), destination, record.Topic, record.Partition, record.Offset).Scan(&found)
	if err != nil {
		return false, fmt.Errorf("control: check skip: %w", err)
	}
	return found, nil
}
