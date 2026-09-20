package httpdelivery

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"
)

type transportGate struct {
	allowed  bool
	deadline time.Time
}

func (g transportGate) Allowed(string) bool      { return g.allowed }
func (g transportGate) LeaseDeadline() time.Time { return g.deadline }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

type closeSignalBody struct{ closed chan struct{} }

func (b closeSignalBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b closeSignalBody) Close() error             { close(b.closed); return nil }

func TestLeaseTransportAppliesLeaseDeadline(t *testing.T) {
	wantDeadline := time.Now().Add(30 * time.Millisecond)
	seen := make(chan error, 1)
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		got, ok := req.Context().Deadline()
		if !ok || got.Before(wantDeadline.Add(-5*time.Millisecond)) || got.After(wantDeadline.Add(5*time.Millisecond)) {
			seen <- errors.New("lease deadline was not copied")
			return nil, errors.New("lease deadline was not copied")
		}
		<-req.Context().Done()
		seen <- req.Context().Err()
		return nil, req.Context().Err()
	})
	req, err := http.NewRequest(http.MethodGet, "http://example.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = (LeaseTransport{Gate: transportGate{allowed: true, deadline: wantDeadline}, Base: base}).RoundTrip(req)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RoundTrip error=%v", err)
	}
	if got := <-seen; !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("base context error=%v", got)
	}
}

func TestLeaseTransportCancelsInFlightBodyWhenClosed(t *testing.T) {
	bodyClosed := make(chan struct{})
	baseSawCancel := make(chan struct{})
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		go func() { <-req.Context().Done(); close(baseSawCancel) }()
		return &http.Response{StatusCode: http.StatusOK, Body: closeSignalBody{closed: bodyClosed}, Request: req}, nil
	})
	req, err := http.NewRequest(http.MethodGet, "http://example.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := (LeaseTransport{Gate: transportGate{allowed: true, deadline: time.Now().Add(time.Minute)}, Base: base}).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-bodyClosed:
	case <-time.After(time.Second):
		t.Fatal("wrapped body did not close")
	}
	select {
	case <-baseSawCancel:
	case <-time.After(time.Second):
		t.Fatal("lease context was not cancelled on body close")
	}
}

func TestLeaseTransportPropagatesRequestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	done := make(chan error, 1)
	go func() {
		_, err := (LeaseTransport{Gate: transportGate{allowed: true, deadline: time.Now().Add(time.Minute)}, Base: base}).RoundTrip(req)
		done <- err
	}()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RoundTrip error=%v", err)
	}
}
