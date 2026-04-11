package intergation_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/Tkach360/tk_cdc/internal/config"
	"github.com/Tkach360/tk_cdc/internal/mapper"
	"github.com/Tkach360/tk_cdc/internal/replicator"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestMain(m *testing.M) {
	// при go test -v будут выводится и логи работы программы
	opts := &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: false,
	}

	handler := slog.NewTextHandler(os.Stdout, opts)
	slog.SetDefault(slog.New(handler))

	os.Exit(m.Run())
}

var (
	ReplicationSlot  = "test_cdc_slot"
	Plugin           = "pgoutput"
	PublicationNames = "test_pub"
)

func TestCDC_CacheInvalidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// запускаем PostgreSQL с логической репликацией
	pgReq := testcontainers.ContainerRequest{
		// TODO: может вынести параметры конфигураций куда-нибудь?
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "testpass",
			"POSTGRES_DB":       "testdb",
		},
		Cmd: []string{
			"postgres",
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=4",
			"-c", "max_wal_senders=4",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
	}

	pgContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: pgReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start postgres: %v", err)
	}
	defer pgContainer.Terminate(ctx)

	pgHost, _ := pgContainer.Host(ctx)
	pgPort, _ := pgContainer.MappedPort(ctx, "5432")
	pgDSN := fmt.Sprintf("postgres://testuser:testpass@%s:%s/testdb", pgHost, pgPort.Port())

	// запускаем Redis
	redisReq := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}

	redisContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: redisReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start redis: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	redisHost, _ := redisContainer.Host(ctx)
	redisPort, _ := redisContainer.MappedPort(ctx, "6379")
	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())

	setupTestDB(ctx, t, pgDSN)

	// инициализация сервиса
	cfg := config.Config{
		Postgres: config.PostgresConfig{
			DSN:              pgDSN,
			ReplicationSlot:  ReplicationSlot,
			Plugin:           Plugin,
			PublicationNames: PublicationNames,
		},
		Redis: config.RedisConfig{
			Addr: redisAddr,
		},
		Mapping: mapper.MappingConfig{
			DefaultSchema: "public",
			Rules: []mapper.MappingRule{
				{Table: mapper.Table{Schema: "public", Name: "users"}, KeyPattern: "user:{id}"},
			},
		},
	}

	rep, err := replicator.New(&cfg)
	if err != nil {
		t.Fatalf("replicator init failed: %v", err)
	}

	// запуск репликатора в фоне
	repCtx, repCancel := context.WithCancel(ctx)
	defer repCancel()

	repErrCh := make(chan error, 1)
	go func() {
		repErrCh <- rep.Run(repCtx)
	}()

	// TODO: стоит ли давать сервису некоторое время для подключения?
	time.Sleep(2 * time.Second)

	// тест UPDATE
	testUpdateInvalidation(ctx, t, pgDSN, redisAddr)

	// тест DELETE
	testDeleteInvalidation(ctx, t, pgDSN, redisAddr)

	repCancel()
	select {
	case err := <-repErrCh:
		if err != nil && err.Error() != "context canceled" {
			t.Logf("replicator finished with error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replicator did not stop gracefully in time")
	}
}

// установить тестовую базу данных
func setupTestDB(ctx context.Context, t *testing.T, dsn string) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("db connect failed: %v", err)
	}
	defer conn.Close(ctx)

	// создаю тестовую таблицу
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		);
	`)
	requireNoErr(t, err, "create table")

	// публикация (обязательно для pgoutput в PG 10+)
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		CREATE PUBLICATION %s FOR ALL TABLES;
	`, PublicationNames))
	requireNoErr(t, err, "create publication")

	// создаю слот
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		SELECT pg_create_logical_replication_slot('%s', '%s');
	`, ReplicationSlot, Plugin))
	// игнорирую ошибку "slot already exists" если тест запускается повторно
	if err != nil && !isAlreadyExists(err) {
		requireNoErr(t, err, "create replication slot")
	}
}

// вспомогательная функция для более удобного вывода
func requireNoErr(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", msg, err)
	}
}

// проверка на ошибку "replication slot already exists"
func isAlreadyExists(err error) bool {
	return err != nil && (err.Error() == fmt.Sprintf("ERROR: replication slot \"%s\" already exists (SQLSTATE 42710)", ReplicationSlot))
}

func testUpdateInvalidation(ctx context.Context, t *testing.T, pgDSN, redisAddr string) {
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	pg, _ := pgx.Connect(ctx, pgDSN)
	defer pg.Close(ctx)

	// вставка строки
	_, err := pg.Exec(ctx, "INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com')")
	requireNoErr(t, err, "insert user")

	// эмулируем кеш приложения
	// TODO: всегда ли ключ будет "user:1"?
	err = rdb.Set(ctx, "user:1", `{"name":"Alice","email":"alice@test.com"}`, 0).Err()
	requireNoErr(t, err, "set redis key")

	// изменяем данные в БД
	_, err = pg.Exec(ctx, "UPDATE users SET email='alice_updated@test.com' WHERE id=1")
	requireNoErr(t, err, "update user")

	// так как CDC асинхронный нужно подождать
	deleted := waitForRedisKey(ctx, t, rdb, "user:1", 5*time.Second)
	if !deleted {
		t.Fatal("Redis key 'user:1' was NOT deleted after UPDATE")
	}
}

func testDeleteInvalidation(ctx context.Context, t *testing.T, pgDSN, redisAddr string) {
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	pg, _ := pgx.Connect(ctx, pgDSN)
	defer pg.Close(ctx)

	_, err := pg.Exec(ctx, "INSERT INTO users (name, email) VALUES ('Bob', 'bob@test.com')")
	requireNoErr(t, err, "insert user")

	// TODO: проблема, всегда ли я буду запускать тесты в такой последовательности? если нет, то нужно как-то узнавать id для эмуляции добавления ключа
	// TODO: ну так сделать SELECT для надежности
	err = rdb.Set(ctx, "user:2", `{"name":"Bob","email":"bob@test.com"}`, 0).Err()
	requireNoErr(t, err, "set redis key")

	_, err = pg.Exec(ctx, "DELETE FROM users WHERE id=2")
	requireNoErr(t, err, "delete user")

	deleted := waitForRedisKey(ctx, t, rdb, "user:2", 5*time.Second)
	if !deleted {
		t.Fatal("Redis key 'user:2' was NOT deleted after DELETE")
	}
}

// опрашивать Redis до удаления ключа или таймаута
func waitForRedisKey(ctx context.Context, t *testing.T, rdb *redis.Client, key string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		exists, err := rdb.Exists(ctx, key).Result()
		if err != nil {
			t.Logf("redis check error: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if exists == 0 {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
