// internal/invalidator/invalidator.go

// логика инвалидации, работа с Redis
package invalidator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Tkach360/tk_cdc/internal/config"
	"github.com/redis/go-redis/v9"
)

type Invalidator struct {
	redis  *redis.Client
	logger *slog.Logger

	maxAttems int
	delay     int
}

func (i *Invalidator) GetQueryDelay() time.Duration {
	return time.Millisecond * time.Duration(i.delay)
}

// создать новый инвалидатор
func New(cfg *config.RedisConfig, logger *slog.Logger) (*Invalidator, error) {
	redis := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Password,
		DB:       cfg.DB,
		// TODO: нужно добавить таймаутов
	})

	inv := &Invalidator{redis, logger, cfg.QMaxAttempts, cfg.QDelay}

	if err := inv.CheckRedis(context.Background()); err != nil {
		return nil, err
	}

	return inv, nil
}

func (i *Invalidator) CheckRedis(ctx context.Context) error {
	if err := i.redis.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping failed: %w", err)
	}
	return nil
}

// инвалидировать ключи
func (i *Invalidator) Invalidate(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	// TODO: сделать sentinel error которая будет хранить неинвалидированный ключ
	var lastErr error
	for attempt := 0; attempt <= i.maxAttems; attempt++ {

		if err := i.redis.Unlink(ctx, keys...).Err(); err == nil {
			return nil
		} else {
			lastErr = err
			i.logger.Error("retrying invalidation", "attempt", attempt, "err", err)
		}

		if attempt == i.maxAttems {
			break
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("invalidation canceled: %w", ctx.Err())
		case <-time.After(i.GetQueryDelay()):
		}
	}
	return fmt.Errorf("invalidation failed: %w", lastErr)
}

// запуск цикла инвалидации
func (i *Invalidator) Run(ctx context.Context, in <-chan []string) error {
	for {
		select {
		case keys, ok := <-in:
			if !ok {
				i.logger.Info("Invalidator: channel closed: exiting")
				return nil
			}

			if err := i.Invalidate(ctx, keys); err != nil {
				return fmt.Errorf("Invalidator: invalidate keys: %w", err)
			}

			i.logger.Info("Invalidator: invalidate keys", "count", len(keys))

		case <-ctx.Done():
			i.logger.Info("Invalidator: context cancelled, exiting")
			return ctx.Err()
		}
	}
}
