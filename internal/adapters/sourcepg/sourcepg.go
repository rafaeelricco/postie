package sourcepg

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/rafaeelricco/postie/internal/provision"
	"github.com/rafaeelricco/postie/internal/stream"
)

// InspectTable connects to the source and reports whether c.Source.Table
// exists in the public schema, its columns and their nullability, and
// whether c.Source.SerialColumn has a single-column unique or primary key
// index. It is read-only and opens and closes its own connection each call.
func (c Client) InspectTable(ctx context.Context) (provision.TableFacts, error) {
	conn, err := pgx.Connect(ctx, connString(c.Connection))
	if err != nil {
		return provision.TableFacts{}, fmt.Errorf("capture: connect to %s: %w", c.Source.ID, err)
	}
	defer conn.Close(ctx)

	var tableExists bool
	err = conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'public' AND c.relname = $1 AND c.relkind IN ('r', 'p')
		)`, c.Source.Table).Scan(&tableExists)
	if err != nil {
		return provision.TableFacts{}, fmt.Errorf("capture: check table public.%s exists: %w", c.Source.Table, err)
	}
	if !tableExists {
		return provision.TableFacts{Exists: false}, nil
	}
	rows, err := conn.Query(ctx, `
		SELECT a.attname, t.typname, a.attnotnull
		FROM pg_attribute a
		JOIN pg_type t ON t.oid = a.atttypid
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relname = $1 AND a.attnum > 0 AND NOT a.attisdropped
	`, c.Source.Table)
	if err != nil {
		return provision.TableFacts{}, fmt.Errorf("capture: read columns of public.%s: %w", c.Source.Table, err)
	}
	columns := map[string]provision.ColumnFacts{}
	for rows.Next() {
		var name, typname string
		var notNull bool
		if err := rows.Scan(&name, &typname, &notNull); err != nil {
			rows.Close()
			return provision.TableFacts{}, fmt.Errorf("capture: read columns of public.%s: %w", c.Source.Table, err)
		}
		columns[name] = provision.ColumnFacts{Type: stream.PGType(typname), NotNull: notNull}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return provision.TableFacts{}, fmt.Errorf("capture: read columns of public.%s: %w", c.Source.Table, err)
	}
	var serialUniqueIndex bool
	err = conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_index i
			JOIN pg_class c ON c.oid = i.indrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
			WHERE n.nspname = 'public' AND c.relname = $1 AND a.attname = $2
			  AND (i.indisunique OR i.indisprimary)
			  AND array_length(i.indkey::int2[], 1) = 1
		)`, c.Source.Table, c.Source.SerialColumn).Scan(&serialUniqueIndex)
	if err != nil {
		return provision.TableFacts{}, fmt.Errorf("capture: check unique index on serialColumn %q: %w", c.Source.SerialColumn, err)
	}
	return provision.TableFacts{Exists: true, Columns: columns, SerialUniqueIndex: serialUniqueIndex}, nil
}

// Connection contains source database credentials and connection settings.
type Connection struct {
	Host     string
	Port     int
	Username string
	Password string
	Database string
}

// Client inspects one configured PostgreSQL source.
type Client struct {
	Source     stream.Source
	Connection Connection
}

func connString(c Connection) string {
	u := url.URL{Scheme: "postgres", User: url.UserPassword(c.Username, c.Password), Host: c.Host + ":" + strconv.Itoa(c.Port), Path: "/" + c.Database}
	return u.String()
}

// SlotHealth reports whether the named replication slot exists and, if so, its
// WAL status. It is read-only and opens and closes its own connection each call.
func (c Client) SlotHealth(ctx context.Context, slot string) (provision.SlotStatus, error) {
	conn, err := pgx.Connect(ctx, connString(c.Connection))
	if err != nil {
		return provision.SlotStatus{}, fmt.Errorf("capture: connect to %s: %w", c.Source.ID, err)
	}
	defer conn.Close(ctx)
	var walStatus *string
	err = conn.QueryRow(ctx, `
		SELECT wal_status
		FROM pg_replication_slots
		WHERE slot_name = $1
	`, slot).Scan(&walStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return provision.SlotStatus{Exists: false}, nil
		}
		return provision.SlotStatus{}, fmt.Errorf("capture: query pg_replication_slots for %q: %w", slot, err)
	}
	status := provision.SlotStatus{Exists: true}
	if walStatus != nil {
		status.WALStatus = provision.WALStatus(*walStatus)
	}
	return status, nil
}
