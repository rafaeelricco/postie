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

type Page struct {
	Entries    []Entry `json:"entries"`
	NextCursor string  `json:"nextCursor"`
	Reset      bool    `json:"reset"`
}

type Log struct {
	mu       sync.Mutex
	boot     string
	sequence uint64
	entries  [1000]Entry
	writer   io.Writer
}

func New(writer io.Writer) *Log { return &Log{boot: rand.Text(), writer: writer} }

func (l *Log) Add(e Entry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sequence++
	e.ID = l.boot + ":" + strconv.FormatUint(l.sequence, 10)
	e.Time = time.Now().UTC()
	if e.Level == "" {
		e.Level = "info"
	}
	l.entries[(l.sequence-1)%1000] = e
	if l.writer != nil {
		_ = json.NewEncoder(l.writer).Encode(e)
	}
}

func (l *Log) Read(cursor string) (Page, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	p := Page{Entries: []Entry{}, NextCursor: l.boot + ":" + strconv.FormatUint(l.sequence, 10)}
	var after uint64
	if cursor != "" {
		parts := strings.Split(cursor, ":")
		if len(parts) != 2 || parts[0] == "" {
			return p, fmt.Errorf("invalid log cursor")
		}
		var err error
		after, err = strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return p, fmt.Errorf("invalid log cursor")
		}
		p.Reset = parts[0] != l.boot || after > l.sequence || l.sequence-after > 1000
		if p.Reset {
			after = 0
		}
	}
	start := uint64(1)
	if l.sequence > 1000 {
		start = l.sequence - 999
	}
	if after >= start {
		start = after + 1
	}
	for n := start; n <= l.sequence; n++ {
		p.Entries = append(p.Entries, l.entries[(n-1)%1000])
	}
	return p, nil
}
