package controlpg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	streams "github.com/rafaeelricco/postie/internal/stream"
)

// Store is the Postgres-backed control-plane store: leases, subscriptions,
// stream registrations, partition markers, and skips. The zero value is not
// usable; construct one with Open.
type Store struct{ pool *pgxpool.Pool }

// schema creates the control tables. Every statement is idempotent, so Open
// applies it on every start.
const schema = `
CREATE TABLE IF NOT EXISTS postie_streams (
    namespace text NOT NULL,
    environment text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    source_id text NOT NULL,
    identity jsonb NOT NULL,
    names jsonb NOT NULL,
    topic_id uuid NOT NULL,
    blocked text NOT NULL DEFAULT '',
    PRIMARY KEY (namespace, environment, generation, source_id)
);
CREATE TABLE IF NOT EXISTS postie_subscriptions (
    namespace text NOT NULL,
    environment text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    destination_id text NOT NULL,
    desired text NOT NULL CHECK (desired IN ('running', 'paused')),
    revision bigint NOT NULL CHECK (revision > 0),
    PRIMARY KEY (namespace, environment, generation, destination_id)
);
CREATE TABLE IF NOT EXISTS postie_worker_leases (
    namespace text NOT NULL,
    environment text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    worker_id text NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (namespace, environment, generation, worker_id)
);
CREATE TABLE IF NOT EXISTS postie_worker_subscriptions (
    namespace text NOT NULL,
    environment text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    worker_id text NOT NULL,
    destination_id text NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    state text NOT NULL CHECK (state <> ''),
    PRIMARY KEY (namespace, environment, generation, worker_id, destination_id)
);
CREATE TABLE IF NOT EXISTS postie_skips (
    namespace text NOT NULL,
    environment text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    destination_id text NOT NULL,
    source_id text NOT NULL,
    event_id text NOT NULL DEFAULT '',
    topic text NOT NULL,
    partition integer NOT NULL,
    offset_value bigint NOT NULL,
    PRIMARY KEY (namespace, environment, generation, destination_id, topic, partition, offset_value)
);
CREATE TABLE IF NOT EXISTS postie_partition_started (
    namespace text NOT NULL,
    environment text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    destination_id text NOT NULL,
    topic text NOT NULL,
    partition integer NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace, environment, generation, destination_id, topic, partition)
);`

// Open connects to databaseURL, verifies the connection with Ping, and
// applies schema. The caller owns the result and must call
// Close. Opening again against an already-initialized database is safe: the
// schema statements are idempotent.
func Open(ctx context.Context, databaseURL string) (*Store, error) {
	if strings.TrimSpace(databaseURL) == "" {
		return nil, errors.New("control: database URL is required")
	}
	p, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("control: open store: %w", err)
	}
	s := &Store{pool: p}
	if err := s.Ping(ctx); err != nil {
		s.Close()
		return nil, err
	}
	if _, err := p.Exec(ctx, schema); err != nil {
		s.Close()
		return nil, fmt.Errorf("control: initialize schema: %w", err)
	}
	return s, nil
}

// Close releases the connection pool. It is safe to call on a nil Store or
// to call more than once.
func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// Ping checks that the store's connection pool can reach the database. It is
// safe to call repeatedly.
func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return errors.New("control: store is closed")
	}
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("control: ping store: %w", err)
	}
	return nil
}

func validScope(scope streams.Scope) error {
	if scope.Namespace == "" || scope.Environment == "" || !scope.Generation.Valid() {
		return fmt.Errorf("control: invalid scope")
	}
	return nil
}

// RenewLease upserts the worker's lease so it expires ttl from now. Calling
// it again before expiry extends the same lease; it never fails because a
// lease already exists.
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

// ReleaseLease removes a worker lease and all of that worker's observations in
// one transaction, so a released worker cannot block a future convergence.
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

// PartitionStarted reports whether MarkPartitionStarted has already been
// recorded for this destination, topic, and partition. It is read-only.
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

// MarkPartitionStarted records that a destination has begun consuming a
// partition. From then on a missing committed offset means history was lost,
// and the consumer blocks the source instead of starting over. It is safe to
// repeat for the same partition.
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

// SaveSkip records that a record reached a terminal outcome for a
// destination without being delivered, so a retry after a crash can tell it
// apart from one never attempted. It is safe to repeat for the same record.
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
