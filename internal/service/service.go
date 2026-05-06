package service

import (
	"context"
	"log/slog"

	"github.com/Tkach360/tk_cdc/internal/config"
	"github.com/Tkach360/tk_cdc/internal/invalidator"
	"github.com/Tkach360/tk_cdc/internal/replicator"
	"golang.org/x/sync/errgroup"
)

type Service struct {
	cfg         *config.Config
	replicator  *replicator.Replicator
	invalidator *invalidator.Invalidator
	logger      *slog.Logger
}

func New(cfg *config.Config, logger *slog.Logger) (*Service, error) {
	replicator, err := replicator.New(cfg, logger)
	if err != nil {
		return nil, err
	}

	invalidator, err := invalidator.New(&cfg.Redis, logger)
	if err != nil {
		return nil, err
	}

	return &Service{cfg, replicator, invalidator, logger}, nil
}

func (s *Service) CheckRedis(ctx context.Context) error {
	return s.invalidator.CheckRedis(ctx)
}

func (s *Service) CheckPostgres(ctx context.Context) error {
	return s.replicator.CheckPostgres(ctx)
}

// запуск сервиса
func (s *Service) Run(ctx context.Context) error {

	keysCh := make(chan []string, 100) // TODO: вынести в конфиг мб
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer close(keysCh)
		return s.replicator.Run(ctx, keysCh)
	})

	g.Go(func() error {
		return s.invalidator.Run(ctx, keysCh)
	})

	return g.Wait()
}
