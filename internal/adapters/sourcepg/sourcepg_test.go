package sourcepg

import (
	"context"
	"strings"
	"testing"

	"github.com/rafaeelricco/postie/internal/stream"
)

// A connection string the driver rejects must not fail Open: provisioning
// tries every source in turn and reports each one's own failure, so an
// unusable source has to surface when it is inspected, not when the adapters
// are built.
func TestOpenDefersAnUnusableConnectionToInspection(t *testing.T) {
	source := stream.Source{ID: "events", Table: "events"}
	client := Open(source, Connection{Host: "localhost", Port: 99999, Username: "u", Password: "p", Database: "d"})
	if client == nil {
		t.Fatal("Open returned nil for a rejected connection string")
	}
	defer client.Close()

	if _, err := client.InspectTable(context.Background()); err == nil {
		t.Fatal("InspectTable succeeded on a client whose pool could not be built")
	} else if !strings.Contains(err.Error(), source.ID) {
		t.Fatalf("InspectTable error %q does not name the source", err)
	}

	if _, err := client.SlotHealth(context.Background(), "slot"); err == nil {
		t.Fatal("SlotHealth succeeded on a client whose pool could not be built")
	} else if !strings.Contains(err.Error(), source.ID) {
		t.Fatalf("SlotHealth error %q does not name the source", err)
	}
}

// Close must not panic on a client that never built a pool.
func TestCloseIsSafeWithoutAPool(t *testing.T) {
	Open(stream.Source{ID: "events"}, Connection{Host: "localhost", Port: 99999}).Close()
}

// A well-formed connection string opens without contacting the database:
// pools connect lazily, so Bootstrap stays fast and offline-safe.
func TestOpenPerformsNoIO(t *testing.T) {
	client := Open(stream.Source{ID: "events"}, Connection{Host: "127.0.0.1", Port: 1, Username: "u", Password: "p", Database: "d"})
	defer client.Close()
	if client.err != nil {
		t.Fatalf("Open rejected a well-formed connection string: %v", client.err)
	}
}
