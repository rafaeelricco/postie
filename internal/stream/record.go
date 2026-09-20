package stream

import "encoding/json"

// Record is the decoded event delivered to destinations.
type Record struct {
	Source      Source
	Payload     json.RawMessage
	Topic       string
	Partition   int32
	Offset      int64
	LeaderEpoch int32
	EventID     string
	Generation  Generation
	Replay      bool
}

// RawRecord is the broker-level event before payload decoding.
type RawRecord struct {
	Topic       string
	Partition   int32
	Offset      int64
	LeaderEpoch int32
	Key         []byte
	Value       []byte
}

// Source is the capture-facing source description shared by engine packages.
// It contains no credentials or connection details.
type Source struct {
	ID                 string
	Description        string
	Table              string
	Columns            []string
	SerialColumn       string
	PartitioningColumn string
}
