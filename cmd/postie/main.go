package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/rafaeelricco/postie/internal/app"
	"github.com/rafaeelricco/postie/internal/config"
	"github.com/rafaeelricco/postie/internal/stream"
)

func main() {
	configPath := flag.String("config", "postie.yaml", "Postie configuration")
	generation := flag.Int("generation", 1, "stream generation")
	flag.Parse()
	engine, application, err := config.Load(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	token, err := config.OperatorToken(os.Getenv("POSTIE_OPERATOR_TOKEN"), engine.Operator.TokenFile, os.ReadFile)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, engine, application, token, stream.Generation(*generation)); err != nil {
		log.Fatal(err)
	}
}
