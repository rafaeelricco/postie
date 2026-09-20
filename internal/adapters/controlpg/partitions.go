package controlpg

import (
	"context"
	"errors"
	"fmt"
	streams "github.com/rafaeelricco/postie/internal/stream"
)

func (s *Store) PartitionStarted(ctx context.Context, scope streams.Scope, destination, topic string, partition int32) (bool, error) {
	if err := validScope(scope); err != nil {
		return false, err
	}
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM postie_partition_started
		WHERE namespace=$1 AND environment=$2 AND generation=$3 AND destination_id=$4 AND topic=$5 AND partition=$6)`,
		scope.Namespace, scope.Environment, int64(scope.Generation), destination, topic, partition).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("control: partition marker: %w", err)
	}
	return exists, nil
}

func (s *Store) MarkPartitionStarted(ctx context.Context, scope streams.Scope, destination, topic string, partition int32) error {
	if err := validScope(scope); err != nil {
		return err
	}
	if destination == "" || topic == "" || partition < 0 {
		return errors.New("control: destination, topic, and non-negative partition are required")
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO postie_partition_started
		(namespace, environment, generation, destination_id, topic, partition) VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT DO NOTHING`, scope.Namespace, scope.Environment, int64(scope.Generation), destination, topic, partition)
	if err != nil {
		return fmt.Errorf("control: mark partition: %w", err)
	}
	return nil
}
