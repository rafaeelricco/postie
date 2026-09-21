package control

import (
	"context"
	"fmt"
	"time"
)

// changePollInterval is how often Change re-checks for convergence.
const changePollInterval = 25 * time.Millisecond

// Change persists a desired state (StateRunning or StatePaused) and waits
// until every live worker has observed it. On error the returned view is the
// best known one, so the caller can still show progress.
//
//	view, err := service.Change(ctx, "billing", control.StatePaused)
//	// err == nil: every worker has stopped delivering to "billing"
func (r *Service) Change(ctx context.Context, id string, state SubscriptionState) (Subscription, error) {
	if _, _, exists := r.snapshot(id); !exists {
		return Subscription{}, ErrNotFound
	}
	desired, err := r.store.SetDesired(ctx, r.scope, id, string(state))
	if err != nil {
		view, _, _ := r.snapshot(id)
		return view, fmt.Errorf("control store unavailable")
	}
	r.Signal()
	return r.awaitConvergence(ctx, id, state, desired.Revision)
}

// awaitConvergence polls until this worker has applied the revision and the
// store confirms every other live worker has too.
func (r *Service) awaitConvergence(ctx context.Context, id string, state SubscriptionState, revision int64) (Subscription, error) {
	ticker := time.NewTicker(changePollInterval)
	defer ticker.Stop()
	for {
		view, applied, _ := r.snapshot(id)
		if applied >= revision && view.State == state && view.DesiredState == state {
			observed, err := r.store.SubscriptionObserved(ctx, r.scope, id, revision, string(state))
			if err != nil {
				return view, fmt.Errorf("worker observation unavailable")
			}
			if observed {
				return view, nil
			}
			// The local worker is done, but the subscription-wide request is pending.
			view.State = pendingState(state)
		}
		// A newer request asking for something else has replaced this one.
		if applied > revision && view.DesiredState != state {
			return view, fmt.Errorf("control request superseded")
		}
		select {
		case <-ctx.Done():
			return view, ctx.Err()
		case <-ticker.C:
		}
	}
}

// snapshot copies a subscription's view and applied revision.
func (r *Service) snapshot(id string) (view Subscription, revision int64, exists bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, exists := r.subscriptions[id]
	if !exists {
		return Subscription{}, 0, false
	}
	return s.view, s.revision, true
}
