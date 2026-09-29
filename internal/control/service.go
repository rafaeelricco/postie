// Package control reconciles durable desired state with observed workers.
package control

import (
	"cmp"
	"context"
	"crypto/rand"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/stream"
)

const (
	// leaseTTL is how long a renewed lease permits dispatch. It is several
	// reconcile intervals long, so one slow reconcile does not stop delivery.
	leaseTTL          = 30 * time.Second
	reconcileInterval = 5 * time.Second
	reconcileTimeout  = 4 * time.Second
	// storeTimeout bounds store calls made outside a reconcile.
	storeTimeout = 3 * time.Second
)

// subscription is the mutable state of one destination. revision is the last
// desired revision this worker has fully applied; 0 means none yet.
type subscription struct {
	view     Subscription
	revision int64
	consumer Consumer
}

// Service owns the control loop of one worker. Fields below mu are guarded by
// it; everything above is fixed after New.
type Service struct {
	sources      []stream.Source
	destinations []Destination
	scope        stream.Scope
	store        Store
	monitor      SourceMonitor
	consumers    ConsumerFactory
	kafka        Health
	log          *activity.Log
	workerID     string
	wake         chan struct{}

	mu            sync.RWMutex
	status        Status
	streams       map[string]stream.Registration
	sourceStates  map[string]SourceStatus
	subscriptions map[string]*subscription
	leaseUntil    time.Time
}

type Options struct {
	Scope        stream.Scope
	Sources      []stream.Source
	Destinations []Destination
	Store        Store
	Monitor      SourceMonitor
	Consumers    ConsumerFactory
	Kafka        Health
	Log          *activity.Log
}

// New registers the destinations as subscriptions and returns a Service in
// the "starting" state. Nothing runs until Start.
//
//	service, err := control.New(ctx, control.Options{...})
//	go service.Start(ctx)     // reconciles until ctx ends
//	service.Change(ctx, "billing", control.StatePaused)
func New(ctx context.Context, options Options) (*Service, error) {
	if !options.Scope.Generation.Valid() {
		return nil, fmt.Errorf("generation must be positive")
	}
	r := &Service{
		scope: options.Scope, sources: options.Sources, destinations: options.Destinations,
		store: options.Store, monitor: options.Monitor, consumers: options.Consumers,
		kafka: options.Kafka, log: options.Log, workerID: rand.Text(),
		status: Status{Status: statusStarting}, streams: map[string]stream.Registration{},
		sourceStates: map[string]SourceStatus{}, subscriptions: map[string]*subscription{},
		wake: make(chan struct{}, 1),
	}
	ids := make([]string, 0, len(r.destinations))
	for _, d := range r.destinations {
		ids = append(ids, d.ID)
		r.subscriptions[d.ID] = &subscription{view: Subscription{ID: d.ID, State: StateStarting}}
	}
	if err := r.store.EnsureSubscriptions(ctx, r.scope, ids); err != nil {
		return nil, fmt.Errorf("subscriptions could not be initialized")
	}
	for _, source := range r.sources {
		r.sourceStates[source.ID] = SourceStatus{ID: source.ID, State: sourceStarting}
	}
	return r, nil
}

// Start reconciles every reconcileInterval, and sooner after Signal, until
// ctx ends. It then stops every consumer and releases the worker lease.
func (r *Service) Start(ctx context.Context) {
	defer r.releaseLease()
	r.log.Add(activity.Entry{Message: "engine started", Generation: int(r.scope.Generation)})
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	for {
		r.reconcile(ctx)
		select {
		case <-ctx.Done():
			r.stopAll()
			return
		case <-ticker.C:
		case <-r.wake:
		}
	}
}

// releaseLease uses a fresh context because Start's is already cancelled.
func (r *Service) releaseLease() {
	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()
	_ = r.store.ReleaseLease(ctx, r.scope, r.workerID)
}

// Signal requests an early reconcile. It never blocks: a pending signal
// already covers the new one.
func (r *Service) Signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Status returns a snapshot with sources sorted by ID.
func (r *Service) Status() Status {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := r.status
	out.Sources = slices.AppendSeq(make([]SourceStatus, 0, len(r.sourceStates)), maps.Values(r.sourceStates))
	slices.SortFunc(out.Sources, func(a, b SourceStatus) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// Subscriptions returns a snapshot sorted by ID.
func (r *Service) Subscriptions() []Subscription {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Subscription, 0, len(r.subscriptions))
	for _, s := range r.subscriptions {
		out = append(out, s.view)
	}
	slices.SortFunc(out, func(a, b Subscription) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// Logs returns the activity entries after cursor; pass "" to read from the oldest kept.
func (r *Service) Logs(cursor string) (activity.Page, error) { return r.log.Read(cursor) }

// LeaseDeadline is when the current lease stops permitting dispatch.
func (r *Service) LeaseDeadline() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaseUntil
}

// Allowed is the dispatch fence. A source may dispatch only while the control
// store and Kafka were healthy at the last reconcile, the lease has not
// expired, and the source is running. Unknown sources are never allowed.
func (r *Service) Allowed(source string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.status.Control && r.status.Kafka && time.Now().Before(r.leaseUntil) && r.sourceStates[source].State == sourceRunning
}

// locked runs fn while holding the write lock. It keeps each critical section
// short and visibly paired, because consumers must never be stopped or
// created while the lock is held.
func (r *Service) locked(fn func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fn()
}

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
