package service

import (
	"context"

	"github.com/Tkach360/tk_cdc/internal/config"
	"github.com/Tkach360/tk_cdc/internal/invalidator"
	"github.com/Tkach360/tk_cdc/internal/replicator"
	"golang.org/x/sync/errgroup"
)

type Service struct {
	cfg         *config.Config
	replicator  *replicator.Replicator
	invalidator *invalidator.Invalidator
}

func New(cfg *config.Config) (*Service, error) {
	replicator, err := replicator.New(cfg)
	if err != nil {
		return nil, err
	}

	invalidator, err := invalidator.New(&cfg.Redis)
	if err != nil {
		return nil, err
	}

	return &Service{cfg, replicator, invalidator}, nil
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
