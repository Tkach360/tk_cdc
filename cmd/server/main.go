// cmd/server/main.go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Tkach360/tk_cdc/internal/config"
	"github.com/Tkach360/tk_cdc/internal/replicator"
)

func main() {
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGTERM,
		syscall.SIGINT,
	)
	defer cancel()

	cfg, err := config.Load("configs/config.yml")
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	slog.Info("starting tk_cdc")

	rep, err := replicator.New(cfg)
	if err != nil {
		slog.Error("failed to creating replicator", "error", err)
		os.Exit(1)
	}

	go func() {
		if err := rep.Run(ctx); err != nil {
			slog.Error("replicator error", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down gracefully")
}
