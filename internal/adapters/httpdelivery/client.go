package httpdelivery

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rafaeelricco/postie/internal/delivery"
	"github.com/rafaeelricco/postie/internal/protocol"
	"github.com/rafaeelricco/postie/internal/stream"
)

// Destination is one HTTP endpoint Postie delivers records to, with the
// basic-auth credentials sent on every request.
type Destination struct {
	ID       string
	Endpoint string
	Username string
	Password string
}

// Client sends records to one Destination over HTTP.
type Client struct {
	HTTP        *http.Client
	Destination Destination
}

// NewClient builds a Client for destination. If client is nil, a default
// *http.Client with a 60-second timeout is used.
func NewClient(client *http.Client, destination Destination) *Client {
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	return &Client{HTTP: client, Destination: destination}
}

// Send POSTs body to the destination for record. Redirects are not followed:
// the first redirect response is returned as-is. The response body is capped
// at 64 KiB; a body over that limit is an error. The status code and body are
// then classified into a delivery.Outcome.
func (c *Client) Send(ctx context.Context, record stream.Record, body []byte) (delivery.Outcome, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Destination.Endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	setHeaders(req, c.Destination, record)
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 60 * time.Second}
	}
	client := *httpClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil {
		return 0, err
	}
	if len(raw) > 64<<10 {
		return 0, fmt.Errorf("destination response exceeds 64 KiB")
	}
	return classify(response.StatusCode, raw)
}

func classify(statusCode int, body []byte) (delivery.Outcome, error) {
	if statusCode < 200 || statusCode > 299 {
		return 0, fmt.Errorf("destination returned HTTP %d", statusCode)
	}
	ack, err := protocol.DecodeAcknowledgement(body)
	if err != nil {
		return 0, err
	}
	switch ack {
	case protocol.Success:
		return delivery.Delivered, nil
	case protocol.KeepGoing:
		return delivery.Skipped, nil
	default:
		return 0, fmt.Errorf("destination requested retry")
	}
}

// Gate reports whether a source's lease still permits an HTTP attempt to
// start, and the deadline that lease is good until.
type Gate interface {
	Allowed(string) bool
	LeaseDeadline() time.Time
}

// LeaseTransport fences each HTTP attempt against the current source lease.
// It bounds an in-flight response by the lease observed when the attempt
// starts and cancels that context when the body is closed.
type LeaseTransport struct {
	Gate   Gate
	Source string
	Base   http.RoundTripper
}

// RoundTrip refuses to start a request once Gate no longer allows Source, and
// otherwise bounds the request's context by the lease deadline observed at
// the start of the attempt, canceling that context when the response body is
// closed.
func (t LeaseTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.Gate != nil && !t.Gate.Allowed(t.Source) {
		return nil, context.Canceled
	}
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	ctx := req.Context()
	var cancel context.CancelFunc
	if t.Gate != nil {
		ctx, cancel = context.WithDeadline(ctx, t.Gate.LeaseDeadline())
	}
	response, err := base.RoundTrip(req.Clone(ctx))
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, err
	}
	if cancel != nil {
		response.Body = &leaseBody{ReadCloser: response.Body, cancel: cancel}
	}
	return response, nil
}

type leaseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *leaseBody) Close() error { defer b.cancel(); return b.ReadCloser.Close() }
