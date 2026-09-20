package control

import (
	"errors"
)

type SubscriptionState string

const (
	StateRunning  SubscriptionState = "running"
	StatePaused   SubscriptionState = "paused"
	StateStarting SubscriptionState = "starting"
	StatePausing  SubscriptionState = "pausing"
	StateBlocked  SubscriptionState = "blocked"
)

type Subscription struct {
	ID           string            `json:"id"`
	State        SubscriptionState `json:"state"`
	DesiredState SubscriptionState `json:"desired_state,omitempty"`
	Error        string            `json:"error,omitempty"`
}
type SourceStatus struct {
	ID    string `json:"id"`
	State string `json:"state"`
	Error string `json:"error,omitempty"`
}
type Status struct {
	Status  string         `json:"status"`
	Kafka   bool           `json:"kafka"`
	Control bool           `json:"control"`
	Sources []SourceStatus `json:"sources"`
}

var ErrNotFound = errors.New("subscription not found")

type DesiredSubscription struct {
	ID       string
	Desired  string
	Revision int64
}
