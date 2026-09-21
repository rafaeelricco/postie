package sourcepg

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/rafaeelricco/postie/internal/provision"
)

// SlotHealth reports whether the named replication slot exists and, if so,
// whether it is active, its WAL status, and how many bytes of WAL it is
// lagging behind the current position. It is read-only and opens and closes
// its own connection each call.
func (c Client) SlotHealth(ctx context.Context, slot string) (provision.SlotStatus, error) {
	conn, err := pgx.Connect(ctx, connString(c.Connection))
	if err != nil {
		return provision.SlotStatus{}, fmt.Errorf("capture: connect to %s: %w", c.Source.ID, err)
	}
	defer conn.Close(ctx)
	var active bool
	var walStatus *string
	var lagBytes *int64
	err = conn.QueryRow(ctx, `
		SELECT active, wal_status, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint
		FROM pg_replication_slots
		WHERE slot_name = $1
	`, slot).Scan(&active, &walStatus, &lagBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return provision.SlotStatus{Exists: false}, nil
		}
		return provision.SlotStatus{}, fmt.Errorf("capture: query pg_replication_slots for %q: %w", slot, err)
	}
	status := provision.SlotStatus{Exists: true, Active: active}
	if walStatus != nil {
		status.WALStatus = provision.WALStatus(*walStatus)
	}
	if lagBytes != nil {
		status.LagBytes = *lagBytes
	}
	return status, nil
}
