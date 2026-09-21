// Package activity keeps a bounded, process-local view of structured engine activity.
// Callers supply identifiers and fixed error categories, never raw dependency errors.
package activity

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Entry is one structured activity record. Zero-valued optional fields are
// omitted from JSON; Partition and Offset are always present because 0 is a
// real partition and a real offset.
type Entry struct {
	ID          string    `json:"id"`
	Time        time.Time `json:"time"`
	Level       string    `json:"level"`
	Message     string    `json:"message"`
	Source      string    `json:"source,omitempty"`
	Destination string    `json:"destination,omitempty"`
	Generation  int       `json:"generation,omitempty"`
	Topic       string    `json:"topic,omitempty"`
	Partition   int32     `json:"partition"`
	Offset      int64     `json:"offset"`
	EventID     string    `json:"event_id,omitempty"`
	AggregateID string    `json:"aggregate_id,omitempty"`
	EventName   string    `json:"event_name,omitempty"`
	Attempt     int       `json:"attempt,omitempty"`
	Outcome     string    `json:"outcome,omitempty"`
	Error       string    `json:"error,omitempty"`
}

// Page is one cursor-based read. Reset tells the reader that its cursor was
// no longer usable and that Entries restarts from the oldest retained entry.
type Page struct {
	Entries    []Entry `json:"entries"`
	NextCursor string  `json:"nextCursor"`
	Reset      bool    `json:"reset"`
}

// capacity is how many entries the ring buffer retains.
const capacity = 1000

// Log is a ring buffer of the most recent entries. Each entry gets a
// sequence number, and entry n lives in slot (n-1) % capacity.
type Log struct {
	mu       sync.Mutex
	boot     string
	sequence uint64
	entries  [capacity]Entry
	writer   io.Writer
}

// New returns an empty log. Every entry is also written to writer as one JSON
// line; pass nil to keep entries in memory only.
//
//	log := activity.New(os.Stdout)
//	log.Add(activity.Entry{Message: "engine started"})
//	page, _ := log.Read("")              // everything retained so far
//	page, _ = log.Read(page.NextCursor)  // only what was added since
func New(writer io.Writer) *Log { return &Log{boot: rand.Text(), writer: writer} }

// Add stamps the entry with an ID and the current time, defaults Level to
// "info", and stores it, evicting the oldest entry once the buffer is full.
func (l *Log) Add(e Entry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sequence++
	e.ID = cursorFor(l.boot, l.sequence)
	e.Time = time.Now().UTC()
	if e.Level == "" {
		e.Level = "info"
	}
	l.entries[slot(l.sequence)] = e
	if l.writer != nil {
		_ = json.NewEncoder(l.writer).Encode(e)
	}
}

// Read returns the entries added after cursor; an empty cursor reads from the
// oldest retained entry. A cursor from another process, from the future, or
// older than the buffer sets Page.Reset and also reads from the oldest entry.
func (l *Log) Read(cursor string) (Page, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	page := Page{Entries: []Entry{}, NextCursor: cursorFor(l.boot, l.sequence)}
	var after uint64
	if cursor != "" {
		boot, sequence, err := parseCursor(cursor)
		if err != nil {
			return page, err
		}
		page.Reset = boot != l.boot || sequence > l.sequence || l.sequence-sequence > capacity
		if !page.Reset {
			after = sequence
		}
	}
	for n := firstUnread(l.sequence, after); n <= l.sequence; n++ {
		page.Entries = append(page.Entries, l.entries[slot(n)])
	}
	return page, nil
}

// cursorFor names a position in the log. The boot prefix is unique per
// process, so a cursor from before a restart is detected instead of trusted.
func cursorFor(boot string, sequence uint64) string {
	return boot + ":" + strconv.FormatUint(sequence, 10)
}

// parseCursor is the inverse of cursorFor.
func parseCursor(cursor string) (boot string, sequence uint64, err error) {
	boot, number, found := strings.Cut(cursor, ":")
	if !found || boot == "" {
		return "", 0, fmt.Errorf("invalid log cursor")
	}
	sequence, err = strconv.ParseUint(number, 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid log cursor")
	}
	return boot, sequence, nil
}

// slot maps a 1-based sequence number to its ring buffer index.
func slot(sequence uint64) uint64 { return (sequence - 1) % capacity }

// firstUnread is the first sequence number to return: the one after the
// cursor, or the oldest entry still retained when the cursor is older.
//
//	firstUnread(5, 0)    // 1
//	firstUnread(5, 3)    // 4
//	firstUnread(2500, 0) // 1501
func firstUnread(sequence, after uint64) uint64 {
	oldest := uint64(1)
	if sequence > capacity {
		oldest = sequence - capacity + 1
	}
	return max(oldest, after+1)
}
