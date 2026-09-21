package delivery

import (
	"encoding/json"

	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/stream"
)

// The functions in this file build activity entries. They are pure: each one
// takes the record's base entry by value and returns a modified copy. Error
// texts are fixed categories, never raw dependency errors, so the operator
// log cannot leak payloads or credentials.

// entryFor is the base entry shared by everything logged about one record.
func entryFor(record stream.Record, destination string) activity.Entry {
	aggregateID, eventName := businessIdentifiers(record.Payload)
	return activity.Entry{
		Source: record.Source.ID, Destination: destination, Generation: int(record.Generation),
		Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
		EventID: record.EventID, AggregateID: aggregateID, EventName: eventName,
	}
}

// businessIdentifiers reads the optional aggregate_id and event_name columns
// so operators can search the log by them. Payloads without them yield "".
func businessIdentifiers(payload json.RawMessage) (aggregateID, eventName string) {
	var identifiers struct {
		AggregateID string `json:"aggregate_id"`
		EventName   string `json:"event_name"`
	}
	_ = json.Unmarshal(payload, &identifiers)
	return identifiers.AggregateID, identifiers.EventName
}

func startedEntry(entry activity.Entry) activity.Entry {
	entry.Message = "delivery started"
	return entry
}

// attemptEntry describes one attempt. attempt is 0-based; the log is 1-based.
func attemptEntry(entry activity.Entry, attempt int, err error) activity.Entry {
	entry.Attempt = attempt + 1
	entry.Message = "acknowledged"
	if err != nil {
		entry.Message = "retry"
		entry.Level = "warn"
		entry.Error = "delivery failed or retry requested"
	}
	return entry
}

func outcomeEntry(entry activity.Entry, outcome Outcome) activity.Entry {
	entry.Message = "terminal outcome"
	entry.Outcome = outcome.String()
	return entry
}

func committedEntry(entry activity.Entry) activity.Entry {
	entry.Message = "committed"
	return entry
}

func commitRetryEntry(entry activity.Entry) activity.Entry {
	entry.Level = "warn"
	entry.Message = "commit retry"
	entry.Error = "Kafka commit unavailable"
	return entry
}
