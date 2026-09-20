// Package delivery contains destination independent retry and delivery
// orchestration. HTTP request construction lives in an adapter.
package delivery

import (
	"context"
	"encoding/json"

	"github.com/rafaeelricco/postie/internal/protocol"
	"github.com/rafaeelricco/postie/internal/stream"
)

func (p *Processor) Process(ctx context.Context, record stream.Record, onAttempt func(int, error)) (Outcome, error) {
	if !protocol.MatchesFilter(p.Destination.Filter, record.Payload) {
		return Filtered, nil
	}
	body, err := json.Marshal(protocol.Envelope{
		DataSourceID: record.Source.ID, DataSourceDescription: record.Source.Description,
		DataDestinationID: p.Destination.ID, DataDestinationDescription: p.Destination.Description,
		Payload: record.Payload,
	})
	if err != nil {
		return 0, err
	}
	for attempt := 0; ; attempt++ {
		outcome, err := p.Sender.Send(ctx, record, body)
		if onAttempt != nil {
			onAttempt(attempt, err)
		}
		if err == nil {
			return outcome, nil
		}
		if err := p.Sleep(ctx, backoff(attempt, p.Jitter)); err != nil {
			return 0, err
		}
	}
}
