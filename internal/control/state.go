package control

import (
	"errors"
)

// SubscriptionState is the state of one destination's delivery on this worker.
//
//	starting -> running           consumer joined and every partition is ready
//	running  -> pausing -> paused operator pause
//	any      -> blocked           a source or dependency is unavailable
type SubscriptionState string

const (
	StateRunning  SubscriptionState = "running"
	StatePaused   SubscriptionState = "paused"
	StateStarting SubscriptionState = "starting"
	StatePausing  SubscriptionState = "pausing"
	StateBlocked  SubscriptionState = "blocked"
)

// Source states. They are plain strings because a SourceMonitor reports them.
const (
	sourceStarting    = "starting"
	sourceRunning     = "running"
	sourceBlocked     = "blocked"
	sourceUnavailable = "unavailable"
)

// Engine status values reported by Status.
const (
	statusStarting = "starting"
	statusReady    = "ready"
	statusDegraded = "degraded"
)

// Subscription is the operator's view of one destination.
type Subscription struct {
	ID           string            `json:"id"`
	State        SubscriptionState `json:"state"`
	DesiredState SubscriptionState `json:"desired_state,omitempty"`
	Error        string            `json:"error,omitempty"`
}

// SourceStatus is the operator's view of one source.
type SourceStatus struct {
	ID    string `json:"id"`
	State string `json:"state"`
	Error string `json:"error,omitempty"`
}

// Status is the engine's health. Status is "ready" only when every source is
// running and every subscription is running or paused.
type Status struct {
	Status  string         `json:"status"`
	Kafka   bool           `json:"kafka"`
	Control bool           `json:"control"`
	Sources []SourceStatus `json:"sources"`
}

var ErrNotFound = errors.New("subscription not found")

// DesiredSubscription is a row of durable desired state. Revision increases on
// every real change, which lets workers tell a new request from an old one.
// The same shape reports what a worker observed, with Desired holding the
// state it reached.
type DesiredSubscription struct {
	ID       string
	Desired  string
	Revision int64
}

func (d DesiredSubscription) paused() bool { return d.Desired == string(StatePaused) }

// overallStatus is "ready" only when nothing needs attention. A paused
// subscription is healthy: the operator asked for it.
func overallStatus(sources map[string]SourceStatus, subscriptions map[string]*subscription) string {
	for _, s := range sources {
		if s.State != sourceRunning {
			return statusDegraded
		}
	}
	for _, s := range subscriptions {
		if s.view.State != StateRunning && s.view.State != StatePaused {
			return statusDegraded
		}
	}
	return statusReady
}

// restingState is where a subscription settles when it must not consume:
// paused when the operator asked for it, blocked (with the reason) otherwise.
func restingState(desired DesiredSubscription, reason string) (SubscriptionState, string) {
	if desired.paused() {
		return StatePaused, ""
	}
	return StateBlocked, reason
}

// pendingState is what a change request reports while this worker is done but
// other workers have not yet observed the revision.
func pendingState(requested SubscriptionState) SubscriptionState {
	if requested == StatePaused {
		return StatePausing
	}
	return StateStarting
}
