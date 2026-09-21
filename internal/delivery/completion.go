package delivery

import (
	"context"
	"time"
)

// recordActions are the side effects needed to finish one record. They are
// injected so completeRecord's ordering can be tested without a broker, a
// store, or a destination.
type recordActions struct {
	skipped func(context.Context) (bool, error) // was a terminal skip already recorded?
	send    func(context.Context) (Outcome, error)
	audit   func(context.Context) error // durably record a terminal skip
	commit  func(context.Context) error
}

// completeRecord runs the actions in the only safe order:
//
//  1. skipped: a record whose skip was already recorded is not sent again.
//  2. send:    deliver until a terminal outcome.
//  3. audit:   a Skipped outcome is recorded before the commit, so a crash
//     between the two cannot lose the fact that the event was skipped.
//  4. commit:  only now may the offset advance.
//
// Store and broker calls are retried every delay until they succeed or ctx
// ends. send does its own retrying, so its error is final.
func completeRecord(ctx context.Context, delay time.Duration, actions recordActions) error {
	var skipped bool
	err := Retry(ctx, delay, func(ctx context.Context) error {
		var err error
		skipped, err = actions.skipped(ctx)
		return err
	})
	if err != nil {
		return err
	}
	if !skipped {
		if err := sendAndAudit(ctx, delay, actions); err != nil {
			return err
		}
	}
	return Retry(ctx, delay, actions.commit)
}

func sendAndAudit(ctx context.Context, delay time.Duration, actions recordActions) error {
	outcome, err := actions.send(ctx)
	if err != nil || outcome != Skipped {
		return err
	}
	return Retry(ctx, delay, actions.audit)
}
