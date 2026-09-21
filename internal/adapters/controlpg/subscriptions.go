package controlpg

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/rafaeelricco/postie/internal/control"
	streams "github.com/rafaeelricco/postie/internal/stream"
	"sort"
	"strings"
)

// EnsureSubscriptions inserts a "running" subscription at revision 1 for
// every ID not already present in the scope. It is safe to repeat: an ID
// that is already registered is left untouched, whatever its current state.
func (s *Store) EnsureSubscriptions(ctx context.Context, scope streams.Scope, ids []string) error {
	if err := validScope(scope); err != nil {
		return err
	}
	ids = append([]string(nil), ids...)
	sort.Strings(ids)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("control: ensure subscriptions: %w", err)
	}
	defer tx.Rollback(ctx)
	for _, id := range ids {
		if id == "" {
			return errors.New("control: subscription ID is required")
		}
		if _, err := tx.Exec(ctx, `INSERT INTO postie_subscriptions
			(namespace, environment, generation, destination_id, desired, revision)
			VALUES ($1,$2,$3,$4,'running',1) ON CONFLICT DO NOTHING`,
			scope.Namespace, scope.Environment, int64(scope.Generation), id); err != nil {
			return fmt.Errorf("control: ensure subscription %q: %w", id, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("control: ensure subscriptions: %w", err)
	}
	return nil
}

// Subscriptions lists every subscription registered for the scope, ordered
// by destination ID. It is read-only.
func (s *Store) Subscriptions(ctx context.Context, scope streams.Scope) ([]control.DesiredSubscription, error) {
	if err := validScope(scope); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT destination_id, desired, revision FROM postie_subscriptions
		WHERE namespace=$1 AND environment=$2 AND generation=$3 ORDER BY destination_id`,
		scope.Namespace, scope.Environment, int64(scope.Generation))
	if err != nil {
		return nil, fmt.Errorf("control: list subscriptions: %w", err)
	}
	defer rows.Close()
	var out []control.DesiredSubscription
	for rows.Next() {
		var sub control.DesiredSubscription
		if err := rows.Scan(&sub.ID, &sub.Desired, &sub.Revision); err != nil {
			return nil, fmt.Errorf("control: list subscriptions: %w", err)
		}
		out = append(out, sub)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("control: list subscriptions: %w", err)
	}
	return out, nil
}

// SetDesired sets the destination's desired state to "running" or "paused"
// and returns the row as stored. The revision only advances when the state
// actually changes, so setting the same state again is a no-op on revision.
func (s *Store) SetDesired(ctx context.Context, scope streams.Scope, id, state string) (control.DesiredSubscription, error) {
	if err := validScope(scope); err != nil {
		return control.DesiredSubscription{}, err
	}
	if state != "running" && state != "paused" {
		return control.DesiredSubscription{}, fmt.Errorf("control: invalid desired state %q", state)
	}
	var sub control.DesiredSubscription
	err := s.pool.QueryRow(ctx, `UPDATE postie_subscriptions SET
		desired=$5, revision=revision + CASE WHEN desired=$5 THEN 0 ELSE 1 END
		WHERE namespace=$1 AND environment=$2 AND generation=$3 AND destination_id=$4
		RETURNING destination_id, desired, revision`, scope.Namespace, scope.Environment, int64(scope.Generation), id, state).
		Scan(&sub.ID, &sub.Desired, &sub.Revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return control.DesiredSubscription{}, fmt.Errorf("control: subscription %q is not registered", id)
	}
	if err != nil {
		return control.DesiredSubscription{}, fmt.Errorf("control: set subscription %q: %w", id, err)
	}
	return sub, nil
}

// ObserveSubscription records the state a worker has reached for a desired
// subscription revision. The observation is kept separately from the lease so
// a newly joined worker cannot count as ready until it has observed the state.
func (s *Store) ObserveSubscription(ctx context.Context, scope streams.Scope, workerID, destination string, revision int64, state string) error {
	if err := validScope(scope); err != nil {
		return err
	}
	if workerID == "" || destination == "" || revision <= 0 || strings.TrimSpace(state) == "" {
		return errors.New("control: worker ID, destination, positive revision, and state are required")
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO postie_worker_subscriptions
		(namespace, environment, generation, worker_id, destination_id, revision, state)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (namespace, environment, generation, worker_id, destination_id)
		DO UPDATE SET revision=EXCLUDED.revision, state=EXCLUDED.state
		WHERE EXCLUDED.revision >= postie_worker_subscriptions.revision`,
		scope.Namespace, scope.Environment, int64(scope.Generation), workerID, destination, revision, state)
	if err != nil {
		return fmt.Errorf("control: observe subscription %q for worker %q: %w", destination, workerID, err)
	}
	return nil
}

// SubscriptionObserved reports whether every currently unexpired worker lease
// for the scope has observed the requested destination revision and state.
// A scope with no unexpired leases is never considered observed.

func (s *Store) SubscriptionObserved(ctx context.Context, scope streams.Scope, destination string, revision int64, state string) (bool, error) {
	if err := validScope(scope); err != nil {
		return false, err
	}
	if destination == "" || revision <= 0 || strings.TrimSpace(state) == "" {
		return false, errors.New("control: destination, positive revision, and state are required")
	}
	var observed bool
	err := s.pool.QueryRow(ctx, `SELECT
		EXISTS (
			SELECT 1 FROM postie_worker_leases
			WHERE namespace=$1 AND environment=$2 AND generation=$3 AND expires_at > now()
		)
		AND NOT EXISTS (
			SELECT 1
			FROM postie_worker_leases AS l
			LEFT JOIN postie_worker_subscriptions AS o
			  ON o.namespace=l.namespace AND o.environment=l.environment AND o.generation=l.generation
			 AND o.worker_id=l.worker_id AND o.destination_id=$4
			WHERE l.namespace=$1 AND l.environment=$2 AND l.generation=$3 AND l.expires_at > now()
			  AND (o.worker_id IS NULL OR o.revision < $5 OR o.state <> $6)
		)`, scope.Namespace, scope.Environment, int64(scope.Generation), destination, revision, state).Scan(&observed)
	if err != nil {
		return false, fmt.Errorf("control: check subscription %q observation: %w", destination, err)
	}
	return observed, nil
}
