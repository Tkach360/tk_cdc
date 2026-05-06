// cmd/server/main.go
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Tkach360/tk_cdc/internal/config"
	"github.com/Tkach360/tk_cdc/internal/service"
)

var (
	ConfigPath = flag.String("config", "./tk_cdc_config.yml", "Service configuration, contains PostgreSQL and Redis settings")
)

func main() {
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGTERM,
		syscall.SIGINT,
	)
	defer cancel()

	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg, err := config.Load(*ConfigPath)
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	logger.Info("starting tk_cdc")

	svc, err := service.New(cfg, logger)
	if err != nil {
		logger.Error("init failed", "err", err)
		os.Exit(1)
	}

	if err := svc.Run(ctx); err != nil && err != context.Canceled {
		logger.Error("service failed", "err", err)
		os.Exit(1)
	}
}
