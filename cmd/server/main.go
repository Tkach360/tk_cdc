// cmd/server/main.go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Tkach360/tk_cdc/internal/config"
	"github.com/Tkach360/tk_cdc/internal/service"
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

	svc, err := service.New(cfg)
	if err != nil {
		slog.Error("init failed", "err", err)
		os.Exit(1)
	}

	if err := svc.Run(ctx); err != nil && err != context.Canceled {
		slog.Error("service failed", "err", err)
		os.Exit(1)
	}
}
