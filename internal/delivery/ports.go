package delivery

import (
	"context"

	"github.com/rafaeelricco/postie/internal/protocol"
	"github.com/rafaeelricco/postie/internal/stream"
)

// Destination is the part of a destination that shapes the envelope. How to
// reach it (endpoint, credentials) belongs to the AttemptSender.
type Destination struct {
	ID          string
	Description string
	Filter      *protocol.Filter
}

// AttemptSender makes one delivery attempt of an encoded envelope. It returns
// a terminal Outcome, or an error when the attempt should be retried.
type AttemptSender interface {
	Send(context.Context, stream.Record, []byte) (Outcome, error)
}

// SkipStore durably remembers terminal skips per destination. After a crash
// between the skip and the offset commit, the record is redelivered; the
// stored skip stops Postie from sending it to the destination a second time.
type SkipStore interface {
	HasSkip(context.Context, stream.Scope, string, stream.Record) (bool, error)
	SaveSkip(context.Context, stream.Scope, string, stream.Record) error
}

// DispatchGate says whether a source may dispatch right now and lets a worker
// block a source whose records cannot be decoded.
type DispatchGate interface {
	Allowed(source string) bool
	Block(source, reason string)
}
