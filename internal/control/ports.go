package control

import (
	"context"
	"github.com/rafaeelricco/postie/internal/stream"
	"time"
)

type Destination struct {
	ID      string
	Sources []string
}

type Store interface {
	GetStream(context.Context, stream.Scope, string) (stream.Registration, bool, error)
	BlockSource(context.Context, stream.Scope, string, string) error
	EnsureSubscriptions(context.Context, stream.Scope, []string) error
	Subscriptions(context.Context, stream.Scope) ([]DesiredSubscription, error)
	SetDesired(context.Context, stream.Scope, string, string) (DesiredSubscription, error)
	RenewLease(context.Context, stream.Scope, string, time.Duration) error
	ObserveSubscription(context.Context, stream.Scope, string, string, int64, string) error
	SubscriptionObserved(context.Context, stream.Scope, string, int64, string) (bool, error)
	ReleaseLease(context.Context, stream.Scope, string) error
}

type SourceMonitor interface {
	InspectSource(context.Context, stream.Source, stream.Registration) (string, string)
}

type Health interface{ Ping(context.Context) error }

type Consumer interface {
	Run()
	Cancel()
	Stop()
	Ready() bool
	Finished() bool
}

type Dispatch interface {
	Allowed(string) bool
	LeaseDeadline() time.Time
	Block(string, string)
	Signal()
}

type ConsumerFactory func(context.Context, Destination, map[string]stream.Registration, Dispatch) (Consumer, error)
