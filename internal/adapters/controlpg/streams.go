package controlpg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	streams "github.com/rafaeelricco/postie/internal/stream"
	"reflect"
	"strings"
)

// GetStream looks up the registration for a source. The bool reports whether
// it was found, independent of the error. It is read-only.
func (s *Store) GetStream(ctx context.Context, scope streams.Scope, sourceID string) (streams.Registration, bool, error) {
	if err := validScope(scope); err != nil {
		return streams.Registration{}, false, err
	}
	var identityRaw, namesRaw []byte
	var id pgtype.UUID
	var blocked string
	err := s.pool.QueryRow(ctx, `SELECT identity, names, topic_id, blocked FROM postie_streams
		WHERE namespace=$1 AND environment=$2 AND generation=$3 AND source_id=$4`,
		scope.Namespace, scope.Environment, int64(scope.Generation), sourceID).Scan(&identityRaw, &namesRaw, &id, &blocked)
	if errors.Is(err, pgx.ErrNoRows) {
		return streams.Registration{}, false, nil
	}
	if err != nil {
		return streams.Registration{}, false, fmt.Errorf("control: get stream %q: %w", sourceID, err)
	}
	var identity streams.Identity
	var names streams.Names
	if err := json.Unmarshal(identityRaw, &identity); err != nil {
		return streams.Registration{}, false, fmt.Errorf("control: decode stream identity: %w", err)
	}
	if err := json.Unmarshal(namesRaw, &names); err != nil {
		return streams.Registration{}, false, fmt.Errorf("control: decode stream names: %w", err)
	}
	return streams.Registration{SourceID: sourceID, Identity: identity, Names: names, TopicID: id.Bytes, Blocked: blocked}, true, nil
}

// RegisterStream inserts a stream registration if none exists yet for the
// source, then verifies under the same transaction that the stored identity,
// names, and topic ID match what was requested. Registering the same source
// with the same identity, names, and topic again is safe; registering it
// with anything different fails instead of silently overwriting the row.
func (s *Store) RegisterStream(ctx context.Context, scope streams.Scope, stream streams.Registration) error {
	if err := validScope(scope); err != nil {
		return err
	}
	if stream.SourceID == "" {
		return errors.New("control: stream source ID is required")
	}
	identity, err := json.Marshal(stream.Identity)
	if err != nil {
		return fmt.Errorf("control: encode stream identity: %w", err)
	}
	names, err := json.Marshal(stream.Names)
	if err != nil {
		return fmt.Errorf("control: encode stream names: %w", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("control: register stream: %w", err)
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `INSERT INTO postie_streams
		(namespace, environment, generation, source_id, identity, names, topic_id, blocked)
		VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7::uuid,$8)
		ON CONFLICT (namespace, environment, generation, source_id) DO NOTHING`,
		scope.Namespace, scope.Environment, int64(scope.Generation), stream.SourceID,
		identity, names, pgtype.UUID{Bytes: stream.TopicID, Valid: true}, stream.Blocked)
	if err != nil {
		return fmt.Errorf("control: register stream: %w", err)
	}
	var oldIdentity, oldNames []byte
	var oldID pgtype.UUID
	var oldBlocked string
	err = tx.QueryRow(ctx, `SELECT identity, names, topic_id, blocked FROM postie_streams
		WHERE namespace=$1 AND environment=$2 AND generation=$3 AND source_id=$4 FOR UPDATE`,
		scope.Namespace, scope.Environment, int64(scope.Generation), stream.SourceID).
		Scan(&oldIdentity, &oldNames, &oldID, &oldBlocked)
	if err != nil {
		return fmt.Errorf("control: read registered stream %q: %w", stream.SourceID, err)
	}
	if !jsonEqual(oldIdentity, identity) || !jsonEqual(oldNames, names) || oldID.Bytes != stream.TopicID {
		return fmt.Errorf("control: stream %q is already registered with different identity, names, or topic", stream.SourceID)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("control: register stream: %w", err)
	}
	return nil
}

// BlockSource marks a registered stream as blocked with the given reason. It
// fails if the source has no registration to update. Calling it again with
// the same or a different reason just overwrites the stored reason.
func (s *Store) BlockSource(ctx context.Context, scope streams.Scope, sourceID, reason string) error {
	if err := validScope(scope); err != nil {
		return err
	}
	if strings.TrimSpace(reason) == "" {
		return errors.New("control: block reason is required")
	}
	tag, err := s.pool.Exec(ctx, `UPDATE postie_streams SET blocked=$5
		WHERE namespace=$1 AND environment=$2 AND generation=$3 AND source_id=$4`,
		scope.Namespace, scope.Environment, int64(scope.Generation), sourceID, reason)
	if err != nil {
		return fmt.Errorf("control: block source %q: %w", sourceID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("control: stream %q is not registered", sourceID)
	}
	return nil
}

func jsonEqual(a, b []byte) bool {
	var av, bv any
	if json.Unmarshal(a, &av) != nil || json.Unmarshal(b, &bv) != nil {
		return reflect.DeepEqual(a, b)
	}
	return reflect.DeepEqual(av, bv)
}
