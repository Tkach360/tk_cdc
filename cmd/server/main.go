// cmd/server/main.go
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Tkach360/tk_cdc/internal/config"
	"github.com/Tkach360/tk_cdc/internal/service"
)

var (
	ConfigPath = flag.String("config", "./tk_cdc_config.yml", "Service configuration, contains PostgreSQL and Redis settings")
	Port       = flag.String("port", "8080", "The port through which the service will receive messages")
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

	go func() {
		if err := svc.Run(ctx); err != nil && err != context.Canceled {
			logger.Error("service failed", "err", err)
			os.Exit(1)
		}
	}()

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{
			"status": "OK",
		})
	})
	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if err := svc.CheckRedis(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]any{
				"status": "not ready",
				"failed": "redis",
				"error":  err.Error(),
			})
			return
		}

		if err := svc.CheckPostgres(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]any{
				"status": "not ready",
				"failed": "postgres",
				"error":  err.Error(),
			})
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "ready",
		})
	})

	logger.Info("starting listening", "port", *Port)
	if err := http.ListenAndServe("0.0.0.0:"+*Port, nil); err != nil {
		logger.Error("listening: ", "error", err)
		os.Exit(1)
	}
}
