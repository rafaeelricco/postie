package activity

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestLogWindowAndCursors(t *testing.T) {
	var output bytes.Buffer
	log := New(&output)
	empty, err := log.Read("")
	if err != nil || empty.Entries == nil || len(empty.Entries) != 0 || empty.Reset {
		t.Fatalf("empty: %+v %v", empty, err)
	}
	for i := 0; i < 1005; i++ {
		log.Add(Entry{Message: "delivered", EventID: fmt.Sprint(i)})
	}
	page, err := log.Read("")
	if err != nil || len(page.Entries) != 1000 || page.Entries[0].EventID != "5" || page.Entries[999].EventID != "1004" {
		t.Fatalf("window: %+v %v", page, err)
	}
	if page.Entries[0].Level != "info" || page.Entries[0].Time.IsZero() || page.Entries[0].ID == "" {
		t.Fatal("missing log metadata")
	}
	if lines := strings.Count(output.String(), "\n"); lines != 1005 {
		t.Fatalf("stdout records: %d", lines)
	}
	if !json.Valid(bytes.Split(output.Bytes(), []byte("\n"))[0]) {
		t.Fatal("stdout is not JSON")
	}
	next, err := log.Read(page.NextCursor)
	if err != nil || len(next.Entries) != 0 || next.Reset {
		t.Fatalf("next: %+v %v", next, err)
	}
	log.Add(Entry{Level: "warn", Message: "retry", Attempt: 1})
	next, err = log.Read(page.NextCursor)
	if err != nil || len(next.Entries) != 1 || next.Entries[0].Level != "warn" {
		t.Fatalf("increment: %+v %v", next, err)
	}
	stale, err := log.Read(empty.NextCursor)
	if err != nil || !stale.Reset || len(stale.Entries) != 1000 {
		t.Fatalf("stale: %+v %v", stale, err)
	}
	restarted, err := New(nil).Read(page.NextCursor)
	if err != nil || !restarted.Reset {
		t.Fatalf("restart: %+v %v", restarted, err)
	}
	for _, cursor := range []string{"broken", "x:no", "x:-1", "x:1:2", ":1"} {
		if _, err := log.Read(cursor); err == nil {
			t.Fatalf("accepted invalid cursor %q", cursor)
		}
	}
	parts := strings.Split(page.NextCursor, ":")
	future, err := log.Read(parts[0] + ":999999")
	if err != nil || !future.Reset {
		t.Fatalf("future: %+v %v", future, err)
	}
}

func TestLogConcurrentReadersOwnTheirSnapshots(t *testing.T) {
	log := New(nil)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				log.Add(Entry{Message: "attempt"})
				page, _ := log.Read("")
				if len(page.Entries) > 0 {
					page.Entries[0].Message = "changed"
				}
			}
		}()
	}
	wg.Wait()
	page, _ := log.Read("")
	if len(page.Entries) != 1000 || page.Entries[0].Message != "attempt" {
		t.Fatal("snapshot mutated log")
	}
}
