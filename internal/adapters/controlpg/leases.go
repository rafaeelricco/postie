package controlpg

import (
	"context"
	"errors"
	"fmt"
	streams "github.com/rafaeelricco/postie/internal/stream"
	"time"
)

func (s *Store) RenewLease(ctx context.Context, scope streams.Scope, workerID string, ttl time.Duration) error {
	if err := validScope(scope); err != nil {
		return err
	}
	if workerID == "" || ttl <= 0 {
		return errors.New("control: worker ID and positive lease TTL are required")
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO postie_worker_leases
		(namespace, environment, generation, worker_id, expires_at) VALUES ($1,$2,$3,$4,now()+$5::interval)
		ON CONFLICT (namespace, environment, generation, worker_id)
		DO UPDATE SET expires_at=EXCLUDED.expires_at`, scope.Namespace, scope.Environment,
		int64(scope.Generation), workerID, ttl.String())
	if err != nil {
		return fmt.Errorf("control: renew lease: %w", err)
	}
	return nil
}

// ObserveSubscription records the state a worker has reached for a desired
// subscription revision. The observation is kept separately from the lease so
// a newly joined worker cannot count as ready until it has observed the state.

func (s *Store) ReleaseLease(ctx context.Context, scope streams.Scope, workerID string) error {
	if err := validScope(scope); err != nil {
		return err
	}
	if workerID == "" {
		return errors.New("control: worker ID is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("control: release lease: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM postie_worker_subscriptions
		WHERE namespace=$1 AND environment=$2 AND generation=$3 AND worker_id=$4`,
		scope.Namespace, scope.Environment, int64(scope.Generation), workerID); err != nil {
		return fmt.Errorf("control: release observations for worker %q: %w", workerID, err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM postie_worker_leases
		WHERE namespace=$1 AND environment=$2 AND generation=$3 AND worker_id=$4`,
		scope.Namespace, scope.Environment, int64(scope.Generation), workerID); err != nil {
		return fmt.Errorf("control: release lease for worker %q: %w", workerID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("control: release lease: %w", err)
	}
	return nil
}
