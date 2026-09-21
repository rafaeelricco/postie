package controlpg

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	streams "github.com/rafaeelricco/postie/internal/stream"
	"strings"
)

// Store is the Postgres-backed control-plane store: leases, subscriptions,
// stream registrations, partition markers, and skips. The zero value is not
// usable; construct one with Open.
type Store struct{ pool *pgxpool.Pool }

//go:embed migrations/001_initial.sql
var schema string

// Open connects to databaseURL, verifies the connection with Ping, and
// applies the embedded schema. The caller owns the result and must call
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
