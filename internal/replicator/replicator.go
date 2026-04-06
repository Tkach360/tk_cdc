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
	"github.com/Tkach360/tk_cdc/internal/invalidator"
	"github.com/Tkach360/tk_cdc/internal/mapper"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

type Replicator struct {
	cfg         *config.Config
	pgConfig    *pgx.ConnConfig
	invalidator *invalidator.Invalidator
	mapper      *mapper.Mapper
	slotName    string
	plugin      string
}

func New(cfg *config.Config) (*Replicator, error) {

	pgConfig, err := pgx.ParseConfig(cfg.Postgres.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse postgres DSN: %w", err)
	}
	pgConfig.RuntimeParams["replication"] = "database"

	invalidator, err := invalidator.New(&cfg.Redis)
	if err != nil {
		return nil, fmt.Errorf("connection redis: %w", err)
	}

	return &Replicator{
		cfg:         cfg,
		pgConfig:    pgConfig,
		invalidator: invalidator,
		mapper:      mapper.New(cfg.Mapping),
		slotName:    cfg.Postgres.ReplicationSlot,
		plugin:      cfg.Postgres.Plugin,
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

				// если сообщение обраотано и транзакция закоммичена
				// то отправляем в postgres уведомление с новым LSN о том что мы обработали
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
// возвращает признак обработки сообщения и ошибку
func (r *Replicator) processLogicalMessage(ctx context.Context, msg pglogrepl.Message) (bool, error) {
	switch msg := msg.(type) {
	case *pglogrepl.BeginMessage:
		slog.Debug("begin transaction", "lsn", msg.FinalLSN, "xid", msg.Xid)
		return false, nil

	case *pglogrepl.CommitMessage:
		slog.Debug("commit transaction", "commit_lsn", msg.CommitLSN, "end_lsn", msg.TransactionEndLSN)
		// TODO: вот тут нужно собственно обновлять данные в redis, так как только в сейчас стало ясно что данные зафиксированы
		// TODO: если транзакция не закоммичена то нужно откатывать изменения
		return true, nil

	case *pglogrepl.RelationMessage:
		slog.Debug("relation metadata", "name", msg.Namespace+"."+msg.RelationName, "oid", msg.RelationID)

		// кешируем названия столбцов при первом обращении к ним в БД так как
		// названия столбцов публикуются только при первой операции, а
		// в WAL postgres при INSERT/UPDATE/DELETE данные о названии столбцов не публикуются
		r.mapper.CacheRelation(msg)
		return false, nil

	case *pglogrepl.InsertMessage:
		// TODO: подойдет ли для идентификации отношения RelationID?
		return r.handleRowChange(ctx, msg.RelationID, msg.Tuple)

	case *pglogrepl.UpdateMessage:
		return r.handleRowChange(ctx, msg.RelationID, msg.NewTuple)

	case *pglogrepl.DeleteMessage:
		return r.handleRowChange(ctx, msg.RelationID, msg.OldTuple)

	case *pglogrepl.LogicalDecodingMessage:
		// пользовательское сообщение
		return false, nil

	default:
		slog.Debug("unhandled message type", "type", fmt.Sprintf("%T", msg))
		return false, nil
	}
}

// обработать изменение записи
func (r *Replicator) handleRowChange(ctx context.Context, relID uint32, tuple *pglogrepl.TupleData) (bool, error) {
	if tuple == nil {
		// TOAST-значения или другие случаи, когда данных нет
		return false, nil
	}

	// получаем ключи redis, которые являются закешированными записями указанного отношения
	keys := r.mapper.GetKeys(relID, tuple)
	if len(keys) == 0 {
		return false, nil
	}

	r.invalidator.Invalidate(ctx, keys)

	// TODO: может каким-то образом доставать имя отношения чтобы логи были более понятными?
	slog.Info("invalidated cache keys", "relID", relID, "keys_count", len(keys))
	return true, nil
}

// отправить статус ожидания
func (r *Replicator) sendStandbyStatusUpdate(ctx context.Context, conn *pgx.Conn, lsn pglogrepl.LSN) error {

	// отправляем статус обработки WAL в postgres, чтобы он смог очистить ненужные записи WAL
	err := pglogrepl.SendStandbyStatusUpdate(
		ctx,
		conn.PgConn(),
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: lsn,
			WALFlushPosition: lsn,
			WALApplyPosition: lsn,
			ClientTime:       time.Now(),
			ReplyRequested:   false,
		},
	)
	if err != nil {
		return fmt.Errorf("send standby status: %w", err)
	}
	return nil
}
