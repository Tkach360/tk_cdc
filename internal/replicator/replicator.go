// internal/replicator/replicator.go

// логика чтения WAL
package replicator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Tkach360/tk_cdc/internal/config"
	"github.com/Tkach360/tk_cdc/internal/mapper"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
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
	slog.Info("connecting to postgres", "dsn", r.cfg.Postgres.DSN)

	conn, err := pgx.ConnectConfig(ctx, r.pgConfig)
	if err != nil {
		return fmt.Errorf("connection postgres: %w", err)
	}
	defer conn.Close(ctx)

	if err := r.ensureReplicationSlot(ctx, conn); err != nil {
		return fmt.Errorf("ensure replication slot: %w", err)
	}

	startLSN, err := r.getRestartLSN(ctx, conn)
	if err != nil {
		return fmt.Errorf("get restart LSN: %w", err)
	}
	slog.Info("starting replication", "slog", r.slotName, "start_lsn", startLSN.String())

	sysident, err := pglogrepl.IdentifySystem(ctx, conn.PgConn())
	if err != nil {
		return fmt.Errorf("ifentify system %w", err)
	}

	// зпауск потока репликации
	err = pglogrepl.StartReplication(
		ctx,
		conn.PgConn(),
		r.slotName,
		startLSN,
		pglogrepl.StartReplicationOptions{PluginArgs: []string{
			"proto_versin '1'",
			fmt.Sprintf("publication_names '%s'", "pgoutput"),
		}},
	)
	if err != nil {
		return fmt.Errorf("start replication %w", err)
	}
	slog.Info("replication stream started", "systemid", sysident.SystemID)

	// основной цикл работы
	// читаем сообщение -> обрабатываем -> подтверждаем обработку
	for {
		select {
		case <-ctx.Done():
			slog.Info("replication stopped by context")
			return ctx.Err()
		default:
		}

		// TODO: вынести величину таймаута в конфиг
		msgCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		rawMsg, err := conn.PgConn().ReceiveMessage(msgCtx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if errors.Is(err, context.Canceled) {
				return nil
			}
			slog.Warn("receive message", "error", err)
			// TODO: нужна логика переподключения
			continue
		}

		switch msg := rawMsg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				// TODO: нужно разобраться как отвечать на сообщения KeepAlive
				continue
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:]) // пропускаем байт типа сообщения
				if err != nil {
					slog.Error("parse xlog data", "error", err)
					continue
				}

				logicalMsg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					slog.Error("parse logical message", "error", err)
					continue
				}

				proceeded, err := r.processLogicalMessage(ctx, logicalMsg)
				if err != nil {
					slog.Error("process message", "error", err, "msg_type", fmt.Sprintf("%T", logicalMsg))
					// TODO: нужно что-то делать в случае ошибки обработки сообщения
				}

				if _, ok := logicalMsg.(*pglogrepl.CommitMessage); proceeded && ok {
					confirmLSN := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
					if err := r.sendStandbyStatusUpdate(ctx, conn, confirmLSN); err != nil {
						slog.Error("send standby status", "error", err)
					}
					slog.Debug("confirmed LSN", "lsn", confirmLSN.String())
				}
			}
		case *pgproto3.ErrorResponse:
			slog.Error("postgres error response", "msg", msg.Message, "severity", msg.Severity)
			return fmt.Errorf("postgres error: %s", msg.Message)
		}
	}
}

// проверить есть ли слот репликации, создать если нет и вернуть его
func (r *Replicator) ensureReplicationSlot(ctx context.Context, conn *pgx.Conn) error {

	// спрашиваем у postgres позицию последнего сообщения, которе мы обработали
	var exists bool
	err := conn.QueryRow(
		ctx,
		"SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
		r.slotName,
	).Scan(&exists)

	if errors.Is(err, pgx.ErrNoRows) {
		// создание слота
		slog.Info("creating replication slot", "slot", r.slotName, "plugin", r.plugin)
		_, err = pglogrepl.CreateReplicationSlot(
			ctx,
			conn.PgConn(),
			r.slotName,
			r.plugin,
			pglogrepl.CreateReplicationSlotOptions{Temporary: false},
		)
		if err != nil {
			return fmt.Errorf("create slot: %w", err)
		}
		slog.Info("replication slot created")
		return nil
	}

	if err != nil {
		return fmt.Errorf("check slot existence: %w", err)
	}

	slog.Info("replication slot exists", "slot", r.slotName)
	return nil
}

// получить позицию LSN с которой следует продолжить чтение
func (r *Replicator) getRestartLSN(ctx context.Context, conn *pgx.Conn) (pglogrepl.LSN, error) {

	// TODO: по-хорошему нужно читать LSN откуда-то, а не использовать начало слота
	var lsnStr string
	err := conn.QueryRow(
		ctx,
		"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
		r.slotName,
	).Scan(&lsnStr)

	if err == pgx.ErrNoRows {
		// новый слот
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("query slot lsn: %w", err)
	}

	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		return 0, fmt.Errorf("parse lsn '%s': %w", lsnStr, err)
	}

	return lsn, nil
}

// обработка логического сообщения
// возвращает признак обработки и ошибку
func (r *Replicator) processLogicalMessage(ctx context.Context, lmsg pglogrepl.Message) (bool, error) {
	// TODO: сделать метод *Replicator.processLogicalMessage
	return true, nil
}

// отправить статус ожидания
func (r *Replicator) sendStandbyStatusUpdate(ctx context.Context, conn *pgx.Conn, cLSN pglogrepl.LSN) error {
	// TODO: сделать метод *Replicator.sendStandbyStatusUpdate
	return nil
}
