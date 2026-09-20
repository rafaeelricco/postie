package control

import (
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/activity"
	"github.com/rafaeelricco/postie/internal/stream"
)

type fakeStore struct {
	mu sync.Mutex

	ensureErr        error
	subscriptionsErr error
	setDesiredErr    error
	renewErr         error
	observeErr       error
	observedErr      error
	blockErr         error

	desired       []DesiredSubscription
	registrations map[string]stream.Registration
	found         map[string]bool
	getErr        map[string]error
	observed      bool

	setDesiredCh chan DesiredSubscription
	observations []DesiredSubscription
	blocked      []string
	renewCalls   int
	releaseCalls int
	releaseCh    chan struct{}
	releaseCheck func() bool
	releaseTTL   time.Duration
	reconcileTTL time.Duration
	blockTTL     time.Duration
}

func newFakeStore(sourceIDs ...string) *fakeStore {
	s := &fakeStore{
		registrations: map[string]stream.Registration{},
		found:         map[string]bool{},
		getErr:        map[string]error{},
		releaseCh:     make(chan struct{}, 1),
	}
	for _, id := range sourceIDs {
		s.registrations[id] = stream.Registration{SourceID: id}
		s.found[id] = true
	}
	return s
}

func (s *fakeStore) GetStream(_ context.Context, _ stream.Scope, id string) (stream.Registration, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.getErr[id]; err != nil {
		return stream.Registration{}, false, err
	}
	reg, ok := s.registrations[id]
	return reg, s.found[id] && ok, nil
}

func (s *fakeStore) BlockSource(ctx context.Context, _ stream.Scope, id, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocked = append(s.blocked, id+":"+reason)
	if deadline, ok := ctx.Deadline(); ok {
		s.blockTTL = time.Until(deadline)
	}
	return s.blockErr
}

func (s *fakeStore) EnsureSubscriptions(_ context.Context, _ stream.Scope, ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ensureErr != nil {
		return s.ensureErr
	}
	if len(s.desired) == 0 {
		for _, id := range ids {
			s.desired = append(s.desired, DesiredSubscription{ID: id, Desired: string(StateRunning), Revision: 1})
		}
	}
	return nil
}

func (s *fakeStore) Subscriptions(_ context.Context, _ stream.Scope) ([]DesiredSubscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subscriptionsErr != nil {
		return nil, s.subscriptionsErr
	}
	return append([]DesiredSubscription(nil), s.desired...), nil
}

func (s *fakeStore) SetDesired(_ context.Context, _ stream.Scope, id, state string) (DesiredSubscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.setDesiredErr != nil {
		return DesiredSubscription{}, s.setDesiredErr
	}
	for i := range s.desired {
		if s.desired[i].ID != id {
			continue
		}
		if s.desired[i].Desired != state {
			s.desired[i].Desired = state
			s.desired[i].Revision++
		}
		out := s.desired[i]
		if s.setDesiredCh != nil {
			select {
			case s.setDesiredCh <- out:
			default:
			}
		}
		return out, nil
	}
	return DesiredSubscription{}, errors.New("missing subscription")
}

func (s *fakeStore) setDesiredState(id, state string, revision int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.desired {
		if s.desired[i].ID == id {
			s.desired[i] = DesiredSubscription{ID: id, Desired: state, Revision: revision}
			return
		}
	}
	s.desired = append(s.desired, DesiredSubscription{ID: id, Desired: state, Revision: revision})
}

func (s *fakeStore) RenewLease(ctx context.Context, _ stream.Scope, _ string, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renewCalls++
	if deadline, ok := ctx.Deadline(); ok {
		s.reconcileTTL = time.Until(deadline)
	}
	return s.renewErr
}

func (s *fakeStore) ObserveSubscription(_ context.Context, _ stream.Scope, _ string, id string, revision int64, state string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observations = append(s.observations, DesiredSubscription{ID: id, Desired: state, Revision: revision})
	return s.observeErr
}

func (s *fakeStore) SubscriptionObserved(_ context.Context, _ stream.Scope, _ string, _ int64, _ string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.observed, s.observedErr
}

func (s *fakeStore) ReleaseLease(ctx context.Context, _ stream.Scope, _ string) error {
	s.mu.Lock()
	s.releaseCalls++
	check := s.releaseCheck
	err := s.blockErr
	if deadline, ok := ctx.Deadline(); ok {
		s.releaseTTL = time.Until(deadline)
	}
	s.mu.Unlock()
	if check != nil && !check() {
		return errors.New("lease released before consumers stopped")
	}
	select {
	case s.releaseCh <- struct{}{}:
	default:
	}
	return err
}

func (s *fakeStore) observationList() []DesiredSubscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]DesiredSubscription(nil), s.observations...)
}

func (s *fakeStore) blockedList() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.blocked...)
}

type fakeMonitor struct {
	mu     sync.Mutex
	state  string
	reason string
	calls  int
}

func (m *fakeMonitor) InspectSource(_ context.Context, _ stream.Source, _ stream.Registration) (string, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	return m.state, m.reason
}

type fakeHealth struct {
	mu  sync.Mutex
	err error
}

func (h *fakeHealth) Ping(context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

type fakeConsumer struct {
	mu           sync.Mutex
	ready        bool
	finished     bool
	canceled     bool
	stopped      bool
	stopOnce     sync.Once
	runStarted   chan struct{}
	runStartOnce sync.Once
	stopCh       chan struct{}
}

func newFakeConsumer(ready bool) *fakeConsumer {
	return &fakeConsumer{ready: ready, runStarted: make(chan struct{}), stopCh: make(chan struct{})}
}

func (c *fakeConsumer) Run() {
	c.runStartOnce.Do(func() { close(c.runStarted) })
	<-c.stopCh
}

func (c *fakeConsumer) Cancel() {
	c.mu.Lock()
	c.canceled = true
	c.mu.Unlock()
}

func (c *fakeConsumer) Stop() {
	c.mu.Lock()
	c.stopped = true
	c.finished = true
	c.mu.Unlock()
	c.stopOnce.Do(func() { close(c.stopCh) })
}

func (c *fakeConsumer) Ready() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ready
}

func (c *fakeConsumer) Finished() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.finished
}

func (c *fakeConsumer) setReady(ready bool) {
	c.mu.Lock()
	c.ready = ready
	c.mu.Unlock()
}

func (c *fakeConsumer) setFinished(finished bool) {
	c.mu.Lock()
	c.finished = finished
	c.mu.Unlock()
}

func (c *fakeConsumer) wasCanceled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.canceled
}

func (c *fakeConsumer) wasStopped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopped
}

type fakeFactory struct {
	mu           sync.Mutex
	queue        []*fakeConsumer
	created      []*fakeConsumer
	destinations []Destination
	err          error
	createdCh    chan *fakeConsumer
}

func (f *fakeFactory) new(_ context.Context, destination Destination, _ map[string]stream.Registration, _ Dispatch) (Consumer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	var c *fakeConsumer
	if len(f.queue) > 0 {
		c = f.queue[0]
		f.queue = f.queue[1:]
	} else {
		c = newFakeConsumer(true)
	}
	f.created = append(f.created, c)
	f.destinations = append(f.destinations, destination)
	if f.createdCh != nil {
		select {
		case f.createdCh <- c:
		default:
		}
	}
	return c, nil
}

func (f *fakeFactory) createdList() []*fakeConsumer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*fakeConsumer(nil), f.created...)
}

func (f *fakeFactory) destinationList() []Destination {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Destination(nil), f.destinations...)
}

func makeService(t *testing.T, sourceIDs []string, destinations []Destination) (*Service, *fakeStore, *fakeMonitor, *fakeHealth, *fakeFactory) {
	t.Helper()
	store := newFakeStore(sourceIDs...)
	monitor := &fakeMonitor{state: "running"}
	health := &fakeHealth{}
	factory := &fakeFactory{}
	sources := make([]stream.Source, 0, len(sourceIDs))
	for _, id := range sourceIDs {
		sources = append(sources, stream.Source{ID: id})
	}
	r, err := New(context.Background(), Options{
		Scope:        stream.Scope{Namespace: "test", Environment: "dev", Generation: 1},
		Sources:      sources,
		Destinations: destinations,
		Store:        store,
		Monitor:      monitor,
		Consumers:    factory.new,
		Kafka:        health,
		Log:          activity.New(io.Discard),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r, store, monitor, health, factory
}

func destination(id string, sources ...string) Destination {
	return Destination{ID: id, Sources: sources}
}

func subscriptionView(r *Service, id string) (Subscription, int64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s := r.subscriptions[id]
	return s.view, s.revision
}

func setSubscription(r *Service, id string, view Subscription, revision int64) {
	r.mu.Lock()
	r.subscriptions[id].view = view
	r.subscriptions[id].revision = revision
	r.mu.Unlock()
}

func TestServiceNewValidationAndErrors(t *testing.T) {
	store := newFakeStore()
	_, err := New(context.Background(), Options{Scope: stream.Scope{Generation: 0}, Store: store})
	if err == nil || !strings.Contains(err.Error(), "generation must be positive") {
		t.Fatalf("invalid generation error = %v", err)
	}
	store.ensureErr = errors.New("database down")
	_, err = New(context.Background(), Options{
		Scope: stream.Scope{Namespace: "test", Environment: "dev", Generation: 1},
		Store: store,
		Log:   activity.New(io.Discard),
	})
	if err == nil || err.Error() != "subscriptions could not be initialized" {
		t.Fatalf("EnsureSubscriptions error = %v", err)
	}
}

func TestServiceStatusSubscriptionsSortingAndLogs(t *testing.T) {
	r, _, _, _, _ := makeService(t, []string{"zeta", "alpha"}, []Destination{destination("zulu"), destination("alpha")})
	status := r.Status()
	if got := []string{status.Sources[0].ID, status.Sources[1].ID}; !sort.StringsAreSorted(got) {
		t.Fatalf("sources not sorted: %v", got)
	}
	subs := r.Subscriptions()
	if got := []string{subs[0].ID, subs[1].ID}; !sort.StringsAreSorted(got) {
		t.Fatalf("subscriptions not sorted: %v", got)
	}
	r.sourceState("alpha", "running", "")
	page, err := r.Logs("")
	if err != nil || len(page.Entries) != 1 || page.Entries[0].Message != "source running" {
		t.Fatalf("logs = %+v, err = %v", page, err)
	}
	if _, err := r.Logs("invalid"); err == nil {
		t.Fatal("invalid log cursor accepted")
	}
}

func TestReconcileStartsReadyConsumerAndObservesRevision(t *testing.T) {
	r, store, _, _, factory := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
	c := newFakeConsumer(true)
	factory.queue = []*fakeConsumer{c}
	defer r.stopAll()

	r.reconcile(context.Background())
	view, revision := subscriptionView(r, "destination")
	if view.State != StateRunning || view.DesiredState != StateRunning || revision != 1 {
		t.Fatalf("subscription = %+v revision=%d", view, revision)
	}
	if r.Status().Status != "ready" || !r.Allowed("events") {
		t.Fatalf("status = %+v, allowed=%v", r.Status(), r.Allowed("events"))
	}
	observations := store.observationList()
	if len(observations) != 1 || observations[0].ID != "destination" || observations[0].Revision != 1 || observations[0].Desired != string(StateRunning) {
		t.Fatalf("observations = %+v", observations)
	}
}

func TestReconcileWaitsForConsumerReadinessAndRecovers(t *testing.T) {
	r, store, _, _, factory := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
	c := newFakeConsumer(false)
	factory.queue = []*fakeConsumer{c}
	defer r.stopAll()

	r.reconcile(context.Background())
	view, revision := subscriptionView(r, "destination")
	if view.State != StateStarting || revision != 0 || len(store.observationList()) != 0 {
		t.Fatalf("not-ready subscription = %+v revision=%d observations=%v", view, revision, store.observationList())
	}
	c.setReady(true)
	r.reconcile(context.Background())
	view, revision = subscriptionView(r, "destination")
	if view.State != StateRunning || revision != 1 {
		t.Fatalf("ready subscription = %+v revision=%d", view, revision)
	}
	c.setReady(false)
	r.reconcile(context.Background())
	view, _ = subscriptionView(r, "destination")
	if view.State != StateStarting {
		t.Fatalf("readiness loss left stale state: %+v", view)
	}
	c.setReady(true)
	r.reconcile(context.Background())
	view, _ = subscriptionView(r, "destination")
	if view.State != StateRunning {
		t.Fatalf("readiness recovery state = %+v", view)
	}
}

func TestReconcilePauseStopAndRestart(t *testing.T) {
	r, store, _, _, factory := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
	c1, c2 := newFakeConsumer(true), newFakeConsumer(true)
	factory.queue = []*fakeConsumer{c1, c2}
	defer r.stopAll()

	r.reconcile(context.Background())
	store.setDesiredState("destination", string(StatePaused), 2)
	r.reconcile(context.Background())
	view, revision := subscriptionView(r, "destination")
	if view.State != StatePaused || revision != 2 || !c1.wasStopped() {
		t.Fatalf("paused subscription = %+v revision=%d stopped=%v", view, revision, c1.wasStopped())
	}
	store.setDesiredState("destination", string(StateRunning), 3)
	r.reconcile(context.Background())
	view, revision = subscriptionView(r, "destination")
	if view.State != StateRunning || revision != 3 || len(factory.createdList()) != 2 {
		t.Fatalf("restarted subscription = %+v revision=%d consumers=%d", view, revision, len(factory.createdList()))
	}
}

func TestReconcileFinishedConsumerIsReplaced(t *testing.T) {
	r, _, _, _, factory := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
	c1, c2 := newFakeConsumer(true), newFakeConsumer(true)
	factory.queue = []*fakeConsumer{c1, c2}
	defer r.stopAll()

	r.reconcile(context.Background())
	c1.setFinished(true)
	r.reconcile(context.Background())
	if len(factory.createdList()) != 2 || !c1.wasStopped() {
		t.Fatalf("finished consumer was not replaced: created=%d stopped=%v", len(factory.createdList()), c1.wasStopped())
	}
	view, _ := subscriptionView(r, "destination")
	if view.State != StateRunning {
		t.Fatalf("replacement state = %+v", view)
	}
}

func TestReconcileConsumerFactoryErrorBlocksSubscription(t *testing.T) {
	r, _, _, _, factory := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
	factory.err = errors.New("kafka unavailable")
	r.reconcile(context.Background())
	view, revision := subscriptionView(r, "destination")
	if view.State != StateBlocked || view.Error != "Kafka consumer unavailable" || revision != 0 {
		t.Fatalf("blocked subscription = %+v revision=%d", view, revision)
	}
	if r.Status().Status != "degraded" {
		t.Fatalf("status = %+v", r.Status())
	}
}

func TestReconcileDependencyAndObservationFailuresStopDispatch(t *testing.T) {
	t.Run("renewal", func(t *testing.T) {
		r, store, _, _, factory := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
		c := newFakeConsumer(true)
		factory.queue = []*fakeConsumer{c}
		r.reconcile(context.Background())
		store.renewErr = errors.New("lease lost")
		r.reconcile(context.Background())
		if !c.wasCanceled() || !c.wasStopped() || r.Status().Status != "degraded" || r.Allowed("events") {
			t.Fatalf("renewal failure: canceled=%v stopped=%v status=%+v allowed=%v", c.wasCanceled(), c.wasStopped(), r.Status(), r.Allowed("events"))
		}
	})
	t.Run("control query", func(t *testing.T) {
		r, store, _, _, factory := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
		c := newFakeConsumer(true)
		factory.queue = []*fakeConsumer{c}
		r.reconcile(context.Background())
		store.subscriptionsErr = errors.New("control unavailable")
		r.reconcile(context.Background())
		if !c.wasStopped() || r.Status().Control || r.Status().Status != "degraded" {
			t.Fatalf("control failure: stopped=%v status=%+v", c.wasStopped(), r.Status())
		}
	})
	t.Run("kafka", func(t *testing.T) {
		r, _, _, health, factory := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
		c := newFakeConsumer(true)
		factory.queue = []*fakeConsumer{c}
		r.reconcile(context.Background())
		health.err = errors.New("broker down")
		r.reconcile(context.Background())
		if !c.wasStopped() || r.Status().Kafka || r.Status().Status != "degraded" {
			t.Fatalf("kafka failure: stopped=%v status=%+v", c.wasStopped(), r.Status())
		}
	})
	t.Run("observation", func(t *testing.T) {
		r, store, _, _, factory := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
		c := newFakeConsumer(true)
		factory.queue = []*fakeConsumer{c}
		r.reconcile(context.Background())
		store.observeErr = errors.New("observation failed")
		r.reconcile(context.Background())
		if !c.wasStopped() || r.Status().Control || r.Status().Status != "degraded" {
			t.Fatalf("observation failure: stopped=%v status=%+v", c.wasStopped(), r.Status())
		}
	})
}

func TestInspectSourceErrorsAndLatchedBlock(t *testing.T) {
	r, store, monitor, _, _ := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
	store.found["events"] = false
	r.reconcile(context.Background())
	if got := r.Status().Sources[0]; got.State != "blocked" || got.Error != "source must be provisioned" {
		t.Fatalf("missing source status = %+v", got)
	}

	store.found["events"] = true
	store.getErr["events"] = errors.New("store unavailable")
	r.reconcile(context.Background())
	if got := r.Status().Sources[0]; got.State != "unavailable" || got.Error != "control state unavailable" {
		t.Fatalf("store error source status = %+v", got)
	}
	delete(store.getErr, "events")

	store.registrations["events"] = stream.Registration{SourceID: "events", Blocked: "history lost"}
	r.reconcile(context.Background())
	if got := r.Status().Sources[0]; got.State != "blocked" || got.Error != "history lost" {
		t.Fatalf("registered block status = %+v", got)
	}

	store.registrations["events"] = stream.Registration{SourceID: "events"}
	r.mu.Lock()
	r.streams = map[string]stream.Registration{}
	r.mu.Unlock()
	monitor.state, monitor.reason = "blocked", "topic history lost"
	r.reconcile(context.Background())
	if got := r.Status().Sources[0]; got.State != "blocked" || got.Error != "topic history lost" {
		t.Fatalf("monitor block status = %+v", got)
	}
	if got := store.blockedList(); len(got) == 0 || got[len(got)-1] != "events:topic history lost" {
		t.Fatalf("blocked calls = %v", got)
	}
	monitor.state, monitor.reason = "running", ""
	r.reconcile(context.Background())
	if got := r.Status().Sources[0]; got.State != "blocked" || got.Error != "topic history lost" {
		t.Fatalf("latched block cleared: %+v", got)
	}
}

func TestChangeConvergesOnlyAfterObservedRevision(t *testing.T) {
	r, store, _, _, _ := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
	store.observed = true
	store.setDesiredCh = make(chan DesiredSubscription, 1)
	result := make(chan struct {
		view Subscription
		err  error
	}, 1)
	go func() {
		view, err := r.Change(context.Background(), "destination", StatePaused)
		result <- struct {
			view Subscription
			err  error
		}{view, err}
	}()
	select {
	case <-store.setDesiredCh:
	case <-time.After(time.Second):
		t.Fatal("Change did not set desired state")
	}
	r.reconcile(context.Background())
	select {
	case got := <-result:
		if got.err != nil || got.view.State != StatePaused || got.view.DesiredState != StatePaused {
			t.Fatalf("Change result = %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Change did not converge")
	}
}

func TestChangeWaitsForUnobservedWorkerAndReportsPendingState(t *testing.T) {
	r, store, _, _, _ := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
	store.setDesiredCh = make(chan DesiredSubscription, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	result := make(chan struct {
		view Subscription
		err  error
	}, 1)
	go func() {
		view, err := r.Change(ctx, "destination", StatePaused)
		result <- struct {
			view Subscription
			err  error
		}{view, err}
	}()
	select {
	case <-store.setDesiredCh:
	case <-time.After(time.Second):
		t.Fatal("Change did not set desired state")
	}
	r.reconcile(context.Background())
	select {
	case got := <-result:
		if !errors.Is(got.err, context.DeadlineExceeded) || got.view.State != StatePausing {
			t.Fatalf("pending Change result = %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Change did not honor timeout")
	}
}

func TestChangeSupersededAndDependencyErrors(t *testing.T) {
	t.Run("superseded", func(t *testing.T) {
		r, store, _, _, _ := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
		store.setDesiredCh = make(chan DesiredSubscription, 1)
		result := make(chan error, 1)
		go func() {
			_, err := r.Change(context.Background(), "destination", StatePaused)
			result <- err
		}()
		select {
		case <-store.setDesiredCh:
		case <-time.After(time.Second):
			t.Fatal("Change did not set desired state")
		}
		setSubscription(r, "destination", Subscription{ID: "destination", State: StateRunning, DesiredState: StateRunning}, 3)
		r.Signal()
		select {
		case err := <-result:
			if err == nil || err.Error() != "control request superseded" {
				t.Fatalf("superseded error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("superseded Change did not return")
		}
	})
	t.Run("set desired", func(t *testing.T) {
		r, store, _, _, _ := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
		store.setDesiredErr = errors.New("write failed")
		view, err := r.Change(context.Background(), "destination", StatePaused)
		if err == nil || err.Error() != "control store unavailable" || view.ID != "destination" {
			t.Fatalf("set desired result = %+v err=%v", view, err)
		}
	})
	t.Run("observation", func(t *testing.T) {
		r, store, _, _, _ := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
		store.observedErr = errors.New("read failed")
		store.setDesiredCh = make(chan DesiredSubscription, 1)
		result := make(chan error, 1)
		go func() {
			_, err := r.Change(context.Background(), "destination", StatePaused)
			result <- err
		}()
		select {
		case <-store.setDesiredCh:
		case <-time.After(time.Second):
			t.Fatal("Change did not set desired state")
		}
		r.reconcile(context.Background())
		select {
		case err := <-result:
			if err == nil || err.Error() != "worker observation unavailable" {
				t.Fatalf("observation error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("observation Change did not return")
		}
	})
	t.Run("missing", func(t *testing.T) {
		r, _, _, _, _ := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
		_, err := r.Change(context.Background(), "missing", StatePaused)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("missing Change error = %v", err)
		}
	})
}

func TestAllowedRejectsExpiredLease(t *testing.T) {
	r, _, _, _, _ := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
	r.mu.Lock()
	r.status = Status{Control: true, Kafka: true}
	r.leaseUntil = time.Now().Add(-time.Second)
	r.sourceStates["events"] = SourceStatus{ID: "events", State: "running"}
	r.mu.Unlock()
	if r.Allowed("events") {
		t.Fatal("expired lease allowed dispatch")
	}
}

func TestStartStopsWorkersBeforeReleasingLease(t *testing.T) {
	r, store, _, _, factory := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
	c := newFakeConsumer(true)
	factory.queue = []*fakeConsumer{c}
	store.releaseCheck = c.wasStopped
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Start(ctx)
		close(done)
	}()
	select {
	case <-c.runStarted:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("consumer did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Start did not stop")
	}
	select {
	case <-store.releaseCh:
	default:
		t.Fatal("Start did not release lease")
	}
	if !c.wasCanceled() || !c.wasStopped() {
		t.Fatalf("consumer shutdown canceled=%v stopped=%v", c.wasCanceled(), c.wasStopped())
	}
	view, _ := subscriptionView(r, "destination")
	if view.State != StateBlocked || view.Error != "engine dependencies unavailable" {
		t.Fatalf("stopped worker view = %+v", view)
	}
	store.mu.Lock()
	releaseTTL := store.releaseTTL
	store.mu.Unlock()
	if releaseTTL < 2900*time.Millisecond || releaseTTL > 3*time.Second {
		t.Fatalf("lease release timeout = %s, want approximately 3s", releaseTTL)
	}
}

func TestReconcileUsesDestinationMatchingDesiredSubscription(t *testing.T) {
	r, _, _, _, factory := makeService(t, []string{"events-a", "events-b"}, []Destination{
		destination("alpha", "events-a"),
		destination("beta", "events-b"),
	})
	defer r.stopAll()
	r.reconcile(context.Background())
	got := factory.destinationList()
	if len(got) != 2 || got[0].ID != "alpha" || got[0].Sources[0] != "events-a" || got[1].ID != "beta" || got[1].Sources[0] != "events-b" {
		t.Fatalf("factory destinations = %+v", got)
	}
}

func TestChangeDoesNotCallEqualRevisionSuperseded(t *testing.T) {
	r, store, _, _, _ := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
	store.setDesiredCh = make(chan DesiredSubscription, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := r.Change(ctx, "destination", StatePaused)
		result <- err
	}()
	select {
	case desired := <-store.setDesiredCh:
		setSubscription(r, "destination", Subscription{
			ID: "destination", State: StateRunning, DesiredState: StateRunning,
		}, desired.Revision)
	case <-time.After(time.Second):
		t.Fatal("Change did not set desired state")
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("equal-revision Change error = %v, want deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Change did not finish after its context expired")
	}
}

func TestReconcileContextHasFourSecondBudget(t *testing.T) {
	r, store, _, _, _ := makeService(t, nil, nil)
	r.reconcile(context.Background())
	store.mu.Lock()
	got := store.reconcileTTL
	store.mu.Unlock()
	if got < 3900*time.Millisecond || got > 4*time.Second {
		t.Fatalf("reconcile budget = %s, want approximately 4s", got)
	}
}

func TestReconcileLogsOnlyStateTransitions(t *testing.T) {
	r, _, _, _, _ := makeService(t, []string{"events"}, []Destination{destination("destination", "events")})
	r.reconcile(context.Background())
	page, err := r.Logs("")
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	countRunning := func(entries []activity.Entry) int {
		count := 0
		for _, entry := range entries {
			if entry.Message == "running" && entry.Destination == "destination" {
				count++
			}
		}
		return count
	}
	if got := countRunning(page.Entries); got != 1 {
		t.Fatalf("initial running transition logs = %d, entries=%+v", got, page.Entries)
	}
	r.reconcile(context.Background())
	page, err = r.Logs(page.NextCursor)
	if err != nil || countRunning(page.Entries) != 0 {
		t.Fatalf("unchanged running state added logs: page=%+v err=%v", page, err)
	}
	r.stopAll()
}

func TestBlockUsesThreeSecondPersistenceBudgetAndLogsOnce(t *testing.T) {
	r, store, _, _, _ := makeService(t, []string{"events"}, nil)
	r.sourceState("events", "running", "")
	r.Block("events", "history lost")
	store.mu.Lock()
	blockTTL := store.blockTTL
	store.mu.Unlock()
	if blockTTL < 2900*time.Millisecond || blockTTL > 3*time.Second {
		t.Fatalf("block persistence timeout = %s, want approximately 3s", blockTTL)
	}
	page, err := r.Logs("")
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	countBlocked := func(entries []activity.Entry) int {
		count := 0
		for _, entry := range entries {
			if entry.Message == "source blocked" && entry.Source == "events" && entry.Error == "history lost" {
				count++
			}
		}
		return count
	}
	if got := countBlocked(page.Entries); got != 1 {
		t.Fatalf("block transition logs = %d, entries=%+v", got, page.Entries)
	}
	r.sourceState("events", "blocked", "history lost")
	page, err = r.Logs("")
	if err != nil || countBlocked(page.Entries) != 1 {
		t.Fatalf("unchanged blocked state added logs: page=%+v err=%v", page, err)
	}
}
