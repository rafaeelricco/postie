package sourcepg

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rafaeelricco/postie/internal/provision"
	"github.com/rafaeelricco/postie/internal/stream"
)

// InspectTable connects to the source and reports whether c.Source.Table
// exists in the public schema, its columns and their nullability, and
// whether c.Source.SerialColumn has a single-column unique or primary key
// index. It is read-only and borrows a pooled connection per query.
func (c *Client) InspectTable(ctx context.Context) (provision.TableFacts, error) {
	var tableExists bool
	err := c.pool.QueryRow(ctx, `
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
	rows, err := c.pool.Query(ctx, `
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
	err = c.pool.QueryRow(ctx, `
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

// Client inspects one configured PostgreSQL source over a pool it owns.
type Client struct {
	Source stream.Source
	pool   *pgxpool.Pool
}

// Open builds a client with its own small pool. The reconcile loop inspects
// every source every few seconds and provisioning polls a slot five times a
// second, so a pool turns each of those from a TCP handshake and a session
// startup into an acquire. The caller must Close it.
func Open(source stream.Source, connection Connection) (*Client, error) {
	config, err := pgxpool.ParseConfig(connString(connection))
	if err != nil {
		return nil, fmt.Errorf("capture: configure pool for %s: %w", source.ID, err)
	}
	// One reconcile inspection and one provisioning poll at a time is the
	// whole access pattern; MinConns stays 0 so Open performs no I/O.
	config.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("capture: open pool for %s: %w", source.ID, err)
	}
	return &Client{Source: source, pool: pool}, nil
}

// Close releases the pool's connections.
func (c *Client) Close() { c.pool.Close() }

func connString(c Connection) string {
	u := url.URL{Scheme: "postgres", User: url.UserPassword(c.Username, c.Password), Host: c.Host + ":" + strconv.Itoa(c.Port), Path: "/" + c.Database}
	return u.String()
}

// SlotHealth reports whether the named replication slot exists and, if so, its
// WAL status. It is read-only and borrows a pooled connection per query.
func (c *Client) SlotHealth(ctx context.Context, slot string) (provision.SlotStatus, error) {
	var walStatus *string
	err := c.pool.QueryRow(ctx, `
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
