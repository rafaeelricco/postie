package delivery

import (
	"context"

	"github.com/rafaeelricco/postie/internal/protocol"
	"github.com/rafaeelricco/postie/internal/stream"
)

type Destination struct {
	ID          string
	Description string
	Filter      *protocol.Filter
}

type AttemptSender interface {
	Send(context.Context, stream.Record, []byte) (Outcome, error)
}
