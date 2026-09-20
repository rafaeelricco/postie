package app

import (
	"context"
	"errors"
	"github.com/rafaeelricco/postie/internal/adapters/operator"
	"github.com/rafaeelricco/postie/internal/config"
	"github.com/rafaeelricco/postie/internal/stream"
	"net/http"
	"os"
	"time"
)

func Run(ctx context.Context, engine config.Engine, application config.Application, token string, generation stream.Generation) error {
	initCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	r, err := New(initCtx, engine, application, generation, os.Stdout)
	cancel()
	if err != nil {
		return err
	}
	defer r.Close()
	// The process log and the operator log are the same structured entries.
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
