package sourcepg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/rafaeelricco/postie/internal/provision"
	"github.com/rafaeelricco/postie/internal/stream"
)

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
