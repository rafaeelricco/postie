// Package control reconciles durable desired state with observed workers.
package control

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/stream"
	"sort"
	"sync"
	"time"
)

const leaseTTL = 30 * time.Second

type subscription struct {
	view     Subscription
	revision int64
	consumer Consumer
}
type Service struct {
	sources       []stream.Source
	destinations  []Destination
	scope         stream.Scope
	store         Store
	monitor       SourceMonitor
	consumers     ConsumerFactory
	kafka         Health
	log           *activity.Log
	workerID      string
	mu            sync.RWMutex
	status        Status
	streams       map[string]stream.Registration
	sourceStates  map[string]SourceStatus
	subscriptions map[string]*subscription
	leaseUntil    time.Time
	wake          chan struct{}
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

func New(ctx context.Context, options Options) (*Service, error) {
	if !options.Scope.Generation.Valid() {
		return nil, fmt.Errorf("generation must be positive")
	}
	r := &Service{
		scope: options.Scope, sources: options.Sources, destinations: options.Destinations,
		store: options.Store, monitor: options.Monitor, consumers: options.Consumers,
		kafka: options.Kafka, log: options.Log, workerID: rand.Text(),
		status: Status{Status: "starting"}, streams: map[string]stream.Registration{},
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
		r.sourceStates[source.ID] = SourceStatus{ID: source.ID, State: "starting"}
	}
	return r, nil
}

func (r *Service) newConsumer(ctx context.Context, destination Destination) (Consumer, error) {
	registered := make(map[string]stream.Registration, len(destination.Sources))
	r.mu.RLock()
	for _, id := range destination.Sources {
		registered[id] = r.streams[id]
	}
	r.mu.RUnlock()
	return r.consumers(ctx, destination, registered, r)
}

func (r *Service) LeaseDeadline() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaseUntil
}

func (r *Service) Start(ctx context.Context) {
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = r.store.ReleaseLease(releaseCtx, r.scope, r.workerID)
	}()
	r.log.Add(activity.Entry{Message: "engine started", Generation: int(r.scope.Generation)})
	ticker := time.NewTicker(5 * time.Second)
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
func (r *Service) Signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}
func (r *Service) Status() Status {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := r.status
	out.Sources = make([]SourceStatus, 0, len(r.sourceStates))
	for _, s := range r.sourceStates {
		out.Sources = append(out.Sources, s)
	}
	sort.Slice(out.Sources, func(i, j int) bool { return out.Sources[i].ID < out.Sources[j].ID })
	return out
}
func (r *Service) Subscriptions() []Subscription {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Subscription, 0, len(r.subscriptions))
	for _, s := range r.subscriptions {
		out = append(out, s.view)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func (r *Service) Logs(cursor string) (activity.Page, error) { return r.log.Read(cursor) }
func (r *Service) Change(ctx context.Context, id string, state SubscriptionState) (Subscription, error) {
	r.mu.RLock()
	_, exists := r.subscriptions[id]
	r.mu.RUnlock()
	if !exists {
		return Subscription{}, ErrNotFound
	}
	desired, err := r.store.SetDesired(ctx, r.scope, id, string(state))
	if err != nil {
		return r.subscriptionView(id), fmt.Errorf("control store unavailable")
	}
	r.Signal()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		r.mu.RLock()
		s := r.subscriptions[id]
		view, revision := s.view, s.revision
		r.mu.RUnlock()
		if revision >= desired.Revision && view.State == state && view.DesiredState == state {
			observed, err := r.store.SubscriptionObserved(ctx, r.scope, id, desired.Revision, string(state))
			if err != nil {
				return view, fmt.Errorf("worker observation unavailable")
			}
			if observed {
				return view, nil
			}
			// The local worker is done, but the subscription-wide request is pending.
			if state == StatePaused {
				view.State = StatePausing
			} else {
				view.State = StateStarting
			}
		}
		if revision > desired.Revision && view.DesiredState != state {
			return view, fmt.Errorf("control request superseded")
		}
		select {
		case <-ctx.Done():
			return view, ctx.Err()
		case <-ticker.C:
		}
	}
}
func (r *Service) subscriptionView(id string) Subscription {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.subscriptions[id].view
}

func (r *Service) reconcile(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, 4*time.Second)
	defer cancel()
	// Renew before using control state; a failed refresh immediately stops dispatch.
	leaseErr := r.store.RenewLease(ctx, r.scope, r.workerID, leaseTTL)
	desired, controlErr := r.store.Subscriptions(ctx, r.scope)
	kafkaErr := r.kafka.Ping(ctx)
	healthy := leaseErr == nil && controlErr == nil && kafkaErr == nil
	r.mu.Lock()
	r.status.Control = leaseErr == nil && controlErr == nil
	r.status.Kafka = kafkaErr == nil
	if leaseErr == nil {
		r.leaseUntil = time.Now().Add(leaseTTL)
	}
	r.mu.Unlock()
	if healthy {
		for _, source := range r.sources {
			r.inspectSource(ctx, source)
		}
	}
	if !healthy || parent.Err() != nil {
		r.mu.Lock()
		r.status.Status = "degraded"
		r.mu.Unlock()
		r.stopAll()
		return
	}
	for _, d := range desired {
		r.mu.Lock()
		s, ok := r.subscriptions[d.ID]
		if !ok {
			r.mu.Unlock()
			continue
		}
		s.view.DesiredState = SubscriptionState(d.Desired)
		c := s.consumer
		r.mu.Unlock()
		var destination Destination
		for _, v := range r.destinations {
			if v.ID == d.ID {
				destination = v
				break
			}
		}
		ready, reason := r.destinationReady(destination)
		if d.Desired == "paused" || !ready {
			if c != nil {
				r.mu.Lock()
				s.view.State = StatePausing
				r.mu.Unlock()
				c.Stop()
				r.mu.Lock()
				s.consumer = nil
				r.mu.Unlock()
			}
			r.mu.Lock()
			previous := s.view.State
			s.view.State = StatePaused
			s.view.Error = ""
			if d.Desired != "paused" {
				s.view.State = StateBlocked
				s.view.Error = reason
			}
			s.revision = d.Revision
			view := s.view
			r.mu.Unlock()
			if previous != view.State {
				r.log.Add(activity.Entry{Message: string(view.State), Destination: d.ID, Error: view.Error})
			}
			continue
		}
		if c != nil && c.Finished() {
			c.Stop()
			r.mu.Lock()
			s.consumer = nil
			r.mu.Unlock()
			c = nil
		}
		if c == nil {
			var err error
			c, err = r.newConsumer(parent, destination)
			if err != nil {
				r.mu.Lock()
				s.view.State = StateBlocked
				s.view.Error = "Kafka consumer unavailable"
				r.mu.Unlock()
				continue
			}
			r.mu.Lock()
			s.consumer = c
			s.view.State = StateStarting
			s.view.Error = ""
			r.mu.Unlock()
			go c.Run()
		}
		if c.Ready() {
			r.mu.Lock()
			changed := s.view.State != StateRunning
			s.view.State = StateRunning
			s.view.Error = ""
			s.revision = d.Revision
			r.mu.Unlock()
			if changed {
				r.log.Add(activity.Entry{Message: "running", Destination: d.ID})
			}
		} else {
			r.mu.Lock()
			s.view.State = StateStarting
			r.mu.Unlock()
		}
	}
	r.mu.RLock()
	observed := make([]DesiredSubscription, 0, len(r.subscriptions))
	for id, sub := range r.subscriptions {
		if sub.revision > 0 {
			observed = append(observed, DesiredSubscription{ID: id, Desired: string(sub.view.State), Revision: sub.revision})
		}
	}
	r.mu.RUnlock()
	for _, sub := range observed {
		if err := r.store.ObserveSubscription(ctx, r.scope, r.workerID, sub.ID, sub.Revision, sub.Desired); err != nil {
			r.mu.Lock()
			r.status.Control = false
			r.status.Status = "degraded"
			r.mu.Unlock()
			r.stopAll()
			return
		}
	}
	r.mu.Lock()
	r.status.Status = "ready"
	for _, s := range r.sourceStates {
		if s.State != "running" {
			r.status.Status = "degraded"
		}
	}
	for _, s := range r.subscriptions {
		if s.view.State != StateRunning && s.view.State != StatePaused {
			r.status.Status = "degraded"
		}
	}
	r.mu.Unlock()
}
func (r *Service) destinationReady(d Destination) (bool, string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, id := range d.Sources {
		if r.sourceStates[id].State != "running" {
			return false, "source unavailable: " + id
		}
	}
	return true, ""
}
func (r *Service) stopAll() {
	r.mu.Lock()
	consumers := []Consumer{}
	for _, s := range r.subscriptions {
		if s.consumer != nil {
			consumers = append(consumers, s.consumer)
			s.consumer.Cancel()
		}
	}
	r.mu.Unlock()
	for _, c := range consumers {
		c.Stop()
	}
	r.mu.Lock()
	for _, s := range r.subscriptions {
		if s.consumer != nil {
			s.consumer = nil
			s.view.State = StateBlocked
			s.view.Error = "engine dependencies unavailable"
		}
	}
	r.mu.Unlock()
}
func (r *Service) Allowed(source string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.status.Control && r.status.Kafka && time.Now().Before(r.leaseUntil) && r.sourceStates[source].State == "running"
}
func (r *Service) sourceState(id, state, reason string) {
	r.mu.Lock()
	old := r.sourceStates[id]
	if blocked := r.streams[id].Blocked; blocked != "" {
		state, reason = "blocked", blocked
	}
	r.sourceStates[id] = SourceStatus{ID: id, State: state, Error: reason}
	r.mu.Unlock()
	if old.State != state || old.Error != reason {
		r.log.Add(activity.Entry{Message: "source " + state, Source: id, Error: reason})
	}
}
func (r *Service) Block(source, reason string) {
	r.mu.Lock()
	stream := r.streams[source]
	stream.Blocked = reason
	r.streams[source] = stream
	r.mu.Unlock()
	r.sourceState(source, "blocked", reason)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = r.store.BlockSource(ctx, r.scope, source, reason)
	r.Signal()
}

// cacheStream preserves a block raised by a worker while inspection was in flight.
func (r *Service) cacheStream(registered stream.Registration) stream.Registration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if blocked := r.streams[registered.SourceID].Blocked; blocked != "" {
		registered.Blocked = blocked
	}
	r.streams[registered.SourceID] = registered
	return registered
}
func (r *Service) inspectSource(ctx context.Context, source stream.Source) {
	r.mu.RLock()
	local := r.streams[source.ID]
	r.mu.RUnlock()
	if local.Blocked != "" {
		_ = r.store.BlockSource(ctx, r.scope, source.ID, local.Blocked)
		return
	}
	registered, found, err := r.store.GetStream(ctx, r.scope, source.ID)
	if err != nil {
		r.sourceState(source.ID, "unavailable", "control state unavailable")
		return
	}
	if !found {
		r.sourceState(source.ID, "blocked", "source must be provisioned")
		return
	}
	registered = r.cacheStream(registered)
	if registered.Blocked != "" {
		r.sourceState(source.ID, "blocked", registered.Blocked)
		return
	}
	state, reason := r.monitor.InspectSource(ctx, source, registered)
	if state == "blocked" {
		r.Block(source.ID, reason)
		return
	}
	r.sourceState(source.ID, state, reason)
}
