package provision

import (
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/rafaeelricco/postie/internal/stream"
)

var validIdentifier = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)

func testSource() stream.Source {
	return stream.Source{ID: "orders", Description: "orders db", Table: "event_store", Columns: []string{"id", "correlation_id", "payload"}, SerialColumn: "id", PartitioningColumn: "correlation_id"}
}

func testIdentity(src stream.Source) stream.Identity {
	columns := make([]stream.Column, len(src.Columns))
	for i, name := range src.Columns {
		columns[i] = stream.Column{Name: name, Type: stream.PGText}
	}
	return stream.Identity{Table: src.Table, SerialColumn: src.SerialColumn, PartitioningColumn: src.PartitioningColumn, Columns: columns}
}

func TestNamesForIsDeterministicAndValid(t *testing.T) {
	cases := []struct {
		name        string
		namespace   string
		environment string
		src         stream.Source
		generation  stream.Generation
	}{
		{"simple", "postie", "production", stream.Source{ID: "orders", Table: "event_store"}, 1},
		{"max-generation", "postie", "production", stream.Source{ID: "orders", Table: "event_store"}, stream.Generation(int(^uint(0) >> 1))},
		{"dots-in-ids", "multi.tenant", "prod.us-east", stream.Source{ID: "src.one", Table: "event_store"}, 2},
		{"uppercase", "Postie", "Production", stream.Source{ID: "Orders-ID", Table: "Event_Store"}, 3},
		{"very-long", "namespace-that-is-quite-long-indeed", "environment-also-rather-long", stream.Source{ID: "a-source-identifier-that-goes-on-and-on-and-on-for-a-while", Table: "event_store"}, 7},
		{"very-long-2", "namespace-that-is-quite-long-indeed", "environment-also-rather-long", stream.Source{ID: "a-source-identifier-that-goes-on-and-on-and-on-for-a-while-too", Table: "event_store"}, 7},
	}
	results := map[string]stream.Names{}
	for _, c := range cases {
		n := NamesFor(c.namespace, c.environment, c.src, c.generation)
		results[c.name] = n
		if !validIdentifier.MatchString(n.Slot) || !validIdentifier.MatchString(n.Publication) {
			t.Errorf("%s: resource name is invalid: %+v", c.name, n)
		}
		if !strings.HasPrefix(n.Slot, "postie_") || !strings.HasPrefix(n.Publication, "postie_") {
			t.Errorf("%s: resource names do not have postie_ prefix: %+v", c.name, n)
		}
		if again := NamesFor(c.namespace, c.environment, c.src, c.generation); again != n {
			t.Errorf("%s: NamesFor is not deterministic: %+v != %+v", c.name, again, n)
		}
		if n.Topic != n.TopicPrefix+".public."+c.src.Table {
			t.Errorf("%s: Topic = %q", c.name, n.Topic)
		}
		if strings.Contains(n.Connector, ".") {
			t.Errorf("%s: Connector %q still contains a dot", c.name, n.Connector)
		}
	}
	if results["very-long"].Slot == results["very-long-2"].Slot || results["very-long"].Publication == results["very-long-2"].Publication {
		t.Fatal("two different long source ids produced the same resource name")
	}
}

func TestIdentityFromRejections(t *testing.T) {
	src := stream.Source{ID: "orders", Table: "event_store", Columns: []string{"id", "correlation_id", "payload", "event_id"}, SerialColumn: "id", PartitioningColumn: "correlation_id"}
	validFacts := func() TableFacts {
		return TableFacts{Exists: true, Columns: map[string]ColumnFacts{
			"id": {Type: stream.PGInt8, NotNull: true}, "correlation_id": {Type: stream.PGText, NotNull: true},
			"payload": {Type: stream.PGText, NotNull: true}, "event_id": {Type: stream.PGText, NotNull: true},
		}, SerialUniqueIndex: true}
	}
	cases := []struct {
		name  string
		facts func() TableFacts
	}{
		{"missing-table", func() TableFacts { f := validFacts(); f.Exists = false; return f }},
		{"missing-column", func() TableFacts { f := validFacts(); delete(f.Columns, "payload"); return f }},
		{"nullable-serial", func() TableFacts { f := validFacts(); f.Columns["id"] = ColumnFacts{Type: stream.PGInt8}; return f }},
		{"text-serial", func() TableFacts {
			f := validFacts()
			f.Columns["id"] = ColumnFacts{Type: stream.PGText, NotNull: true}
			return f
		}},
		{"no-unique-index-on-serial", func() TableFacts { f := validFacts(); f.SerialUniqueIndex = false; return f }},
		{"nullable-partitioning", func() TableFacts {
			f := validFacts()
			f.Columns["correlation_id"] = ColumnFacts{Type: stream.PGText}
			return f
		}},
		{"jsonb", func() TableFacts {
			f := validFacts()
			f.Columns["payload"] = ColumnFacts{Type: stream.PGType("jsonb"), NotNull: true}
			return f
		}},
		{"uuid", func() TableFacts {
			f := validFacts()
			f.Columns["payload"] = ColumnFacts{Type: stream.PGType("uuid"), NotNull: true}
			return f
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := IdentityFrom(src, c.facts(), 5)
			if err == nil {
				t.Fatal("identityFrom succeeded, want an error")
			}
			var contractErr *ContractError
			if !errors.As(err, &contractErr) {
				t.Fatalf("identityFrom error = %v (%T), want *ContractError", err, err)
			}
			if contractErr.Table != src.Table {
				t.Errorf("ContractError.Table = %q, want %q", contractErr.Table, src.Table)
			}
		})
	}
	got, err := IdentityFrom(src, validFacts(), 5)
	if err != nil {
		t.Fatal(err)
	}
	want := stream.Identity{Table: "event_store", SerialColumn: "id", PartitioningColumn: "correlation_id", EventIDColumn: "event_id", Partitions: 5, Columns: []stream.Column{{Name: "id", Type: stream.PGInt8}, {Name: "correlation_id", Type: stream.PGText}, {Name: "payload", Type: stream.PGText}, {Name: "event_id", Type: stream.PGText}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("identityFrom = %+v, want %+v", got, want)
	}
}

func TestIdentityFromAcceptsNullableJSON(t *testing.T) {
	src := testSource()
	facts := TableFacts{Exists: true, Columns: map[string]ColumnFacts{
		"id":             {Type: stream.PGInt8, NotNull: true},
		"correlation_id": {Type: stream.PGText, NotNull: true},
		"payload":        {Type: stream.PGJSON},
	}, SerialUniqueIndex: true}

	got, err := IdentityFrom(src, facts, 3)
	if err != nil {
		t.Fatalf("IdentityFrom() error = %v", err)
	}
	want := testIdentity(src)
	want.Partitions = 3
	want.Columns[0].Type = stream.PGInt8
	want.Columns[2].Type = stream.PGJSON
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("IdentityFrom() = %+v, want %+v", got, want)
	}
}
