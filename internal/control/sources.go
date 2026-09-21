package control

import (
	"context"

	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/stream"
)

// inspectSource refreshes one source's state. A block is a latch: once a
// source is blocked, locally or in the store, inspection never reopens it.
func (r *Service) inspectSource(ctx context.Context, source stream.Source) {
	r.mu.RLock()
	local := r.streams[source.ID]
	r.mu.RUnlock()
	if local.Blocked != "" {
		// Block may have failed to reach the store; keep trying.
		_ = r.store.BlockSource(ctx, r.scope, source.ID, local.Blocked)
		return
	}
	registered, found, err := r.store.GetStream(ctx, r.scope, source.ID)
	if err != nil {
		r.sourceState(source.ID, sourceUnavailable, "control state unavailable")
		return
	}
	if !found {
		r.sourceState(source.ID, sourceBlocked, "source must be provisioned")
		return
	}
	registered = r.cacheStream(registered)
	if registered.Blocked != "" {
		r.sourceState(source.ID, sourceBlocked, registered.Blocked)
		return
	}
	state, reason := r.monitor.InspectSource(ctx, source, registered)
	if state == sourceBlocked {
		r.Block(source.ID, reason)
		return
	}
	r.sourceState(source.ID, state, reason)
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

// sourceState records a source's state and logs it when it changed. A latched
// block overrides whatever the caller observed.
func (r *Service) sourceState(id, state, reason string) {
	var old SourceStatus
	r.locked(func() {
		old = r.sourceStates[id]
		if blocked := r.streams[id].Blocked; blocked != "" {
			state, reason = sourceBlocked, blocked
		}
		r.sourceStates[id] = SourceStatus{ID: id, State: state, Error: reason}
	})
	if old.State != state || old.Error != reason {
		r.log.Add(activity.Entry{Message: "source " + state, Source: id, Error: reason})
	}
}

// Block latches a source as blocked: in memory first, so dispatch stops at
// once, then in the store, so other workers and restarts see it too.
func (r *Service) Block(source, reason string) {
	r.locked(func() {
		registered := r.streams[source]
		registered.Blocked = reason
		r.streams[source] = registered
	})
	r.sourceState(source, sourceBlocked, reason)
	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()
	_ = r.store.BlockSource(ctx, r.scope, source, reason)
	r.Signal()
}

// destinationReady reports whether every source of the destination is running.
func (r *Service) destinationReady(d Destination) (ready bool, reason string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, id := range d.Sources {
		if r.sourceStates[id].State != sourceRunning {
			return false, "source unavailable: " + id
		}
	}
	return true, ""
}
