// Package regression replays fixed corpora through the real delivery path so
// a bug fix becomes a permanent test by adding a file, not by writing Go.
package regression

import (
	"context"
	"errors"
	"github.com/rafaeelricco/postie/internal/adapters/httpdelivery"
	"github.com/rafaeelricco/postie/internal/stream"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/config"
	"github.com/rafaeelricco/postie/internal/delivery"
)

// errRetried is returned by a Sleep stub so that "the sender would retry"
// becomes an observable result instead of an infinite loop.
var errRetried = errors.New("regression: sender slept before a retry")

// TestAckCorpus serves every file in testdata/acks as the body of a 200
// response and checks that delivery.Sender classifies it the way the file
// name promises. The name carries the expectation as the prefix before
// "__": delivered, skipped, or retry. Adding a regression case is adding a
// file; no Go changes.
func TestAckCorpus(t *testing.T) {
	const dir = "testdata/acks"
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	if len(entries) == 0 {
		t.Fatalf("%s: empty corpus directory", dir)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		t.Run(name, func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatalf("reading %s: %v", name, err)
			}
			prefix, _, ok := strings.Cut(name, "__")
			if !ok {
				t.Fatalf("%s: file name has no \"__\" separator to carry the expectation", name)
			}

			receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write(body)
			}))
			defer receiver.Close()
			destination := config.Destination{ID: "d", Description: "d", Endpoint: receiver.URL, Username: "u", Password: "p"}

			sender := delivery.NewProcessor(delivery.Destination{ID: destination.ID, Description: destination.Description}, httpdelivery.NewClient(nil, httpdelivery.Destination{ID: destination.ID, Endpoint: destination.Endpoint, Username: destination.Username, Password: destination.Password}))
			switch prefix {
			case "delivered", "skipped":
				// A terminal outcome must not sleep: any Sleep call here
				// means classification did not stop the retry loop.
				sender.Sleep = func(context.Context, time.Duration) error {
					t.Fatal("unexpected retry sleep")
					return nil
				}
			case "retry":
				sender.Sleep = func(context.Context, time.Duration) error {
					return errRetried
				}
			default:
				t.Fatalf("%s: unknown expectation prefix %q; want delivered, skipped, or retry", name, prefix)
			}

			record := stream.Record{Source: stream.Source{ID: "s", Description: "s"}, Payload: []byte(`{}`), Generation: 1}
			outcome, err := sender.Process(context.Background(), record, nil)

			switch prefix {
			case "delivered":
				if err != nil || outcome != delivery.Delivered {
					t.Fatalf("Process() = %v, %v; want Delivered, nil", outcome, err)
				}
			case "skipped":
				if err != nil || outcome != delivery.Skipped {
					t.Fatalf("Process() = %v, %v; want Skipped, nil", outcome, err)
				}
			case "retry":
				if !errors.Is(err, errRetried) {
					t.Fatalf("Process() = %v, %v; want the retry sleep to fire (errRetried)", outcome, err)
				}
			}
		})
	}
}
