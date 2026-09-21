package control

import (
	"context"
	"time"

	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/stream"
)

// reconcile is one pass of the control loop:
//
//  1. Renew the lease and check the dependencies. A failure stops dispatch.
//  2. Inspect every source.
//  3. Move each subscription toward its desired state.
//  4. Report what this worker reached, so Change can see every worker agree.
//  5. Publish the overall status.
//
// Any dependency failure degrades the engine and stops every consumer; the
// next pass starts them again once the dependencies are back.
func (r *Service) reconcile(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, reconcileTimeout)
	defer cancel()
	desired, healthy := r.refreshDependencies(ctx)
	if healthy {
		for _, source := range r.sources {
			r.inspectSource(ctx, source)
		}
	}
	if !healthy || parent.Err() != nil {
		r.degrade()
		return
	}
	for _, d := range desired {
		// Consumers outlive this pass, so they are tied to parent and not ctx.
		r.reconcileSubscription(parent, d)
	}
	if !r.reportObservations(ctx) {
		r.locked(func() { r.status.Control = false })
		r.degrade()
		return
	}
	r.locked(func() { r.status.Status = overallStatus(r.sourceStates, r.subscriptions) })
}

// refreshDependencies renews the lease before anything else uses control
// state: a failed refresh must stop dispatch immediately.
func (r *Service) refreshDependencies(ctx context.Context) (desired []DesiredSubscription, healthy bool) {
	leaseErr := r.store.RenewLease(ctx, r.scope, r.workerID, leaseTTL)
	desired, controlErr := r.store.Subscriptions(ctx, r.scope)
	kafkaErr := r.kafka.Ping(ctx)
	controlHealthy := leaseErr == nil && controlErr == nil
	kafkaHealthy := kafkaErr == nil
	r.locked(func() {
		r.status.Control = controlHealthy
		r.status.Kafka = kafkaHealthy
		if leaseErr == nil {
			r.leaseUntil = time.Now().Add(leaseTTL)
		}
	})
	return desired, controlHealthy && kafkaHealthy
}

// degrade marks the engine degraded and stops every consumer.
func (r *Service) degrade() {
	r.locked(func() { r.status.Status = statusDegraded })
	r.stopAll()
}

// reconcileSubscription moves one subscription one step toward desired.
func (r *Service) reconcileSubscription(parent context.Context, desired DesiredSubscription) {
	var s *subscription
	var consumer Consumer
	r.locked(func() {
		if s = r.subscriptions[desired.ID]; s != nil {
			s.view.DesiredState = SubscriptionState(desired.Desired)
			consumer = s.consumer
		}
	})
	if s == nil {
		return // the store knows a destination this configuration does not
	}
	destination := findDestination(r.destinations, desired.ID)
	ready, reason := r.destinationReady(destination)
	if desired.paused() || !ready {
		r.rest(s, consumer, desired, reason)
		return
	}
	consumer, ok := r.ensureConsumer(parent, s, consumer, destination)
	if !ok {
		return
	}
	r.markProgress(s, desired, consumer.Ready())
}

// rest stops the subscription's consumer and settles it as paused or blocked.
// Reaching a resting state applies the revision: this worker has done what
// was asked.
func (r *Service) rest(s *subscription, consumer Consumer, desired DesiredSubscription, reason string) {
	if consumer != nil {
		r.locked(func() { s.view.State = StatePausing })
		consumer.Stop()
		r.locked(func() { s.consumer = nil })
	}
	var previous SubscriptionState
	var view Subscription
	r.locked(func() {
		previous = s.view.State
		s.view.State, s.view.Error = restingState(desired, reason)
		s.revision = desired.Revision
		view = s.view
	})
	if previous != view.State {
		r.log.Add(activity.Entry{Message: string(view.State), Destination: desired.ID, Error: view.Error})
	}
}

// ensureConsumer returns the subscription's live consumer, replacing one
// that has finished and starting one when there is none. ok is false when no
// consumer could be created; the subscription is then blocked until the next
// pass.
func (r *Service) ensureConsumer(parent context.Context, s *subscription, consumer Consumer, destination Destination) (_ Consumer, ok bool) {
	if consumer != nil && consumer.Finished() {
		consumer.Stop()
		r.locked(func() { s.consumer = nil })
		consumer = nil
	}
	if consumer != nil {
		return consumer, true
	}
	consumer, err := r.newConsumer(parent, destination)
	if err != nil {
		r.locked(func() { s.view.State, s.view.Error = StateBlocked, "Kafka consumer unavailable" })
		return nil, false
	}
	r.locked(func() {
		s.consumer = consumer
		s.view.State, s.view.Error = StateStarting, ""
	})
	go consumer.Run()
	return consumer, true
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

// markProgress records whether the consumer is ready. Only a ready consumer
// applies the revision: "starting" is not what the operator asked for.
func (r *Service) markProgress(s *subscription, desired DesiredSubscription, ready bool) {
	if !ready {
		r.locked(func() { s.view.State = StateStarting })
		return
	}
	var changed bool
	r.locked(func() {
		changed = s.view.State != StateRunning
		s.view.State, s.view.Error = StateRunning, ""
		s.revision = desired.Revision
	})
	if changed {
		r.log.Add(activity.Entry{Message: "running", Destination: desired.ID})
	}
}

// reportObservations stores the state this worker reached for every applied
// revision. It reports false on the first store failure.
func (r *Service) reportObservations(ctx context.Context) bool {
	for _, o := range r.observations() {
		if err := r.store.ObserveSubscription(ctx, r.scope, r.workerID, o.ID, o.Revision, o.Desired); err != nil {
			return false
		}
	}
	return true
}

func (r *Service) observations() []DesiredSubscription {
	r.mu.RLock()
	defer r.mu.RUnlock()
	observed := make([]DesiredSubscription, 0, len(r.subscriptions))
	for id, s := range r.subscriptions {
		if s.revision > 0 {
			observed = append(observed, DesiredSubscription{ID: id, Desired: string(s.view.State), Revision: s.revision})
		}
	}
	return observed
}

// stopAll cancels every consumer first, so they wind down in parallel, then
// waits for each one outside the lock, and finally marks them blocked.
func (r *Service) stopAll() {
	var consumers []Consumer
	r.locked(func() {
		for _, s := range r.subscriptions {
			if s.consumer != nil {
				consumers = append(consumers, s.consumer)
				s.consumer.Cancel()
			}
		}
	})
	for _, c := range consumers {
		c.Stop()
	}
	r.locked(func() {
		for _, s := range r.subscriptions {
			if s.consumer != nil {
				s.consumer = nil
				s.view.State, s.view.Error = StateBlocked, "engine dependencies unavailable"
			}
		}
	})
}

// findDestination returns the configured destination, or the zero value.
func findDestination(destinations []Destination, id string) Destination {
	for _, d := range destinations {
		if d.ID == id {
			return d
		}
	}
	return Destination{}
}
