// internal/invalidator/invalidator.go

// логика инвалидации, работа с Redis
package invalidator

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Tkach360/tk_cdc/internal/config"
	"github.com/redis/go-redis/v9"
)

type Invalidator struct {
	redis *redis.Client
}

// создать новый инвалидатор
func New(cfg *config.RedisConfig) (*Invalidator, error) {
	redis := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
		// TODO: нужно добавить таймаутов
	})

	if err := redis.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return &Invalidator{redis}, nil
}

// инвалидировать ключи
func (i *Invalidator) Invalidate(ctx context.Context, keys []string) error {
	// удаляем ключи из redis
	// TODO: может использовать redis.Pipeliner для удаления? нужно разобраться что лучше
	i.redis.Del(ctx, keys...)

	// TODO: нужно делать повторные попытки удалить если соединение с redis было прервано
	return nil
}

// запуск цикла инвалидации
func (i *Invalidator) Run(ctx context.Context, in <-chan []string) error {
	for {
		select {
		case keys, ok := <-in:
			if !ok {
				slog.Info("Invalidator: channel closed: exiting")
				return nil
			}

			if err := i.Invalidate(ctx, keys); err != nil {
				return fmt.Errorf("Invalidator: invalidate keys: %w", err)
			}

			slog.Info("Invalidator: invalidate keys", "count", len(keys))

		case <-ctx.Done():
			slog.Info("Invalidator: context cancelled, exiting")
			return ctx.Err()
		}
	}
}
