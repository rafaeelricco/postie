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

type Store struct{ pool *pgxpool.Pool }

//go:embed migrations/001_initial.sql
var schema string

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

func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

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
