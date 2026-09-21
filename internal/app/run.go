package app

import (
	"context"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/rafaeelricco/postie/internal/adapters/operator"
	"github.com/rafaeelricco/postie/internal/config"
	"github.com/rafaeelricco/postie/internal/stream"
)

// Run starts an engine under a bounded startup timeout, then serves the
// operator API until ctx is canceled or the server itself fails. On
// shutdown the worker is stopped before the HTTP server is drained, so no
// in-flight delivery is cut short by the server closing first.
// http.ErrServerClosed from a clean Shutdown is reported as a nil error.
func Run(ctx context.Context, engine config.Engine, application config.Application, token string, generation stream.Generation) error {
	initCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	r, err := New(initCtx, engine, application, generation, os.Stdout)
	cancel()
	if err != nil {
		return err
	}
	defer r.Close()
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan struct{})
	go func() { defer close(done); r.Start(workerCtx) }()
	server := &http.Server{Addr: engine.Operator.Listen, Handler: operator.New(token, r).Handler(), ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 40 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()
	select {
	case <-ctx.Done():
	case err = <-serveErr:
	}
	stop()
	<-done
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), engine.Delivery.DrainTimeout)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
