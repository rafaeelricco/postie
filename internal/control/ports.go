package control

import (
	"context"
	"time"

	"github.com/rafaeelricco/postie/internal/stream"
)

// Destination is a subscription and the sources it consumes.
type Destination struct {
	ID      string
	Sources []string
}

// Store is the durable control state shared by every worker of a scope.
type Store interface {
	GetStream(context.Context, stream.Scope, string) (stream.Registration, bool, error)
	BlockSource(ctx context.Context, scope stream.Scope, source, reason string) error
	EnsureSubscriptions(ctx context.Context, scope stream.Scope, destinations []string) error
	Subscriptions(context.Context, stream.Scope) ([]DesiredSubscription, error)
	SetDesired(ctx context.Context, scope stream.Scope, destination, state string) (DesiredSubscription, error)
	RenewLease(ctx context.Context, scope stream.Scope, worker string, ttl time.Duration) error
	ObserveSubscription(ctx context.Context, scope stream.Scope, worker, destination string, revision int64, state string) error
	SubscriptionObserved(ctx context.Context, scope stream.Scope, destination string, revision int64, state string) (bool, error)
	ReleaseLease(ctx context.Context, scope stream.Scope, worker string) error
}

// SourceMonitor checks an established stream and returns its state
// ("running", "blocked", or "unavailable") with a user-facing reason.
type SourceMonitor interface {
	InspectSource(context.Context, stream.Source, stream.Registration) (state, reason string)
}

// Health is any dependency that can be pinged.
type Health interface{ Ping(context.Context) error }

// Consumer is one destination's running delivery. Run blocks until the
// consumer ends. Cancel asks it to end without waiting; Stop waits.
type Consumer interface {
	Run()
	Cancel()
	Stop()
	Ready() bool
	Finished() bool
}

// Dispatch is what the Service offers to consumers and workers: the fence
// they must check before dispatching, and a way to report a broken source.
type Dispatch interface {
	Allowed(source string) bool
	LeaseDeadline() time.Time
	Block(source, reason string)
	Signal()
}

// ConsumerFactory builds the consumer for a destination from the current
// registrations of its sources.
type ConsumerFactory func(context.Context, Destination, map[string]stream.Registration, Dispatch) (Consumer, error)
