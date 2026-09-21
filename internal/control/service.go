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
