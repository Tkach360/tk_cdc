// internal/replicator/replicator.go

// логика чтения WAL
package replicator

import (
	"context"
	"fmt"

	"github.com/Tkach360/tk_cdc/internal/config"
	"github.com/Tkach360/tk_cdc/internal/mapper"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

type Replicator struct {
	cfg      *config.Config
	pgConfig *pgx.ConnConfig
	redis    *redis.Client
	mapper   *mapper.Mapper
	slotName string
	plugin   string
}

func New(cfg *config.Config) (*Replicator, error) {

	pgConfig, err := pgx.ParseConfig(cfg.Postgres.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse postgres DSN: %w", err)
	}

	pgConfig.RuntimeParams["replication"] = "database"

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return &Replicator{
		cfg:      cfg,
		pgConfig: pgConfig,
		redis:    rdb,
		mapper:   mapper.New(cfg.Mapping),
		slotName: cfg.Postgres.ReplicationSlot,
		plugin:   cfg.Postgres.Plugin,
	}, nil
}

// основной цикл работы replicator
// - читает WAL
// - обновляет кеш
func (r *Replicator) Run(ctx context.Context) error {
	// TODO: сделать метод Replicator.Run
	return nil
}
