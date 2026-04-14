package intergation_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Tkach360/tk_cdc/internal/config"
	"github.com/Tkach360/tk_cdc/internal/replicator"
	"github.com/docker/go-connections/nat"
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

type ContainersConfig struct {
	pgImage     string
	pgAdminUser string
	pgAdminPass string
	pgDBName    string
	pgPort      nat.Port

	rdImage string
	rdPort  nat.Port
	rdPass  string
	rdDB    int

	repUser   string
	repPass   string
	appUser   string
	appPass   string
	publNames string
	repSlot   string

	configYML string
}

// TODO: переделать на Options
func insertDefaultFields(s *ContainersConfig) {
	if s.pgImage == "" {
		s.pgImage = "postgres:16-alpine"
	}
	if s.pgAdminUser == "" {
		s.pgAdminUser = "default_admin"
	}
	if s.pgAdminPass == "" {
		s.pgAdminPass = "default_admin_pass"
	}
	if s.pgDBName == "" {
		s.pgDBName = "default_test_db"
	}
	if s.pgPort == "" {
		s.pgPort = nat.Port("5432")
	}
	if s.rdImage == "" {
		s.rdImage = "redis:7-alpine"
	}
	if s.rdPort == "" {
		s.rdPort = nat.Port("6379")
	}
	if s.rdPass == "" {
		s.rdPass = "default_redis_pass"
	}
	if s.repUser == "" {
		s.repUser = "default_replication"
	}
	if s.repPass == "" {
		s.repPass = "default_replication_pass"
	}
	if s.appUser == "" {
		s.appUser = "default_app_user"
	}
	if s.appPass == "" {
		s.appPass = "default_app_pass"
	}
	if s.publNames == "" {
		s.publNames = "default_publNames"
	}
	if s.repSlot == "" {
		s.repSlot = "default_rep_slot" // не допускаются заглавные буквы
	}
	if s.configYML == "" {
		s.configYML = `
postgres:
  addr: "localhost:5432"
  db: "default_test_db"
  replication_user: "default_replication"
  replication_pass: "default_replication_pass"
  app_user: "default_app_user"
  app_pass: "default_app_pass"
  replication_slot: "default_rep_slot"
  plugin: "pgoutput"
  publication_names: "default_publNames"
redis:
  addr: "localhost:6379"
  password: "default_redis_pass"
  db: 0
mapping:
  default_schema: "public"
  rules:
   - table: "users"
     key_pattern: "user:{id}"
`
	}
}

func (s *ContainersConfig) initContainers(ctx context.Context) (testcontainers.Container, testcontainers.Container, error) {

	pgContainer, err := s.createPostgresContainer(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("creating postgres: %w", err)
	}

	pgHost, _ := pgContainer.Host(ctx)
	pgPort, _ := pgContainer.MappedPort(ctx, s.pgPort)
	pgAddr := fmt.Sprintf("%s:%s", pgHost, pgPort.Port())
	adminDSN := fmt.Sprintf("postgres://postgres:%s@%s/%s?sslmode=disable", s.pgAdminPass, pgAddr, s.pgDBName)

	err = s.setupDBForService(ctx, adminDSN)
	if err != nil {
		return nil, nil, fmt.Errorf("setup database: %w", err)
	}

	rdContainer, err := s.createRedisContainer(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("creating redis: %w", err)
	}

	return pgContainer, rdContainer, nil
}

// создать контейнер с postgres
func (s *ContainersConfig) createPostgresContainer(ctx context.Context) (testcontainers.Container, error) {
	// запускаем PostgreSQL с логической репликацией
	pgReq := testcontainers.ContainerRequest{
		Image:        s.pgImage,
		ExposedPorts: []string{s.pgPort.Port() + "/tcp"},
		Env: map[string]string{
			"POSTGRES_PASSWORD": s.pgAdminPass,
			"POSTGRES_DB":       s.pgDBName,
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
		return nil, fmt.Errorf("failed to start postgres: %v", err)
	}

	return pgContainer, nil
}

// создать контейнер с redis
func (s *ContainersConfig) createRedisContainer(ctx context.Context) (testcontainers.Container, error) {
	// запускаем Redis
	redisReq := testcontainers.ContainerRequest{
		Image:        s.rdImage,
		ExposedPorts: []string{s.rdPort.Port() + "/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}

	redisContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: redisReq,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start redis: %w", err)
	}

	return redisContainer, nil
}

// нстроить базу данных для сервиса
func (s *ContainersConfig) setupDBForService(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("db connect failed: %w", err)
	}
	defer conn.Close(ctx)

	// создание пользователя для репликации
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		CREATE USER %s WITH LOGIN PASSWORD '%s' REPLICATION;
		GRANT CONNECT ON DATABASE %s TO %s;
	`, s.repUser, s.repPass, s.pgDBName, s.repUser))
	if err != nil {
		return fmt.Errorf("with creating replication user: %w", err)
	}

	// создание пользователя для чтение LSN и проверки слота репликации
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		CREATE USER %s WITH LOGIN PASSWORD '%s' REPLICATION;
		GRANT CONNECT ON DATABASE %s TO %s;
		GRANT SELECT ON pg_replication_slots TO %s;
		`, s.appUser, s.appPass, s.pgDBName, s.appUser, s.appUser))
	if err != nil {
		return fmt.Errorf("with creating app user: %w", err)
	}

	// публикация (обязательно для pgoutput в PG 10+)
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		CREATE PUBLICATION %s FOR ALL TABLES;
	`, s.publNames))
	if err != nil {
		return fmt.Errorf("create publication: %w", err)
	}

	// создаю слот
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		SELECT pg_create_logical_replication_slot('%s', 'pgoutput');
	`, s.repSlot))

	// проверка на ошибку "replication slot already exists"
	isAlreadyExists := func(err error) bool {
		return err != nil && (err.Error() == fmt.Sprintf("ERROR: replication slot \"%s\" already exists (SQLSTATE 42710)", s.repSlot))
	}

	// игнорирую ошибку "slot already exists" если тест запускается повторно
	if err != nil && !isAlreadyExists(err) {
		return fmt.Errorf("create replication slot: %w", err)
	}

	return nil
}

// функция для тестирования
// emulateWork - функция, эмулирующая работу postgres и redis, принимает их соединения
// checkFunc - вызывается после того, как emulateWork отработала и проверяет состаяние redis
func (s *ContainersConfig) TestFunc(
	ctx context.Context,
	t *testing.T,
	emulateWork func(ctx context.Context, t *testing.T, pgConn *pgx.Conn, rdConn *redis.Client),
	checkFunc func(ctx context.Context, t *testing.T, rdConn *redis.Client)) {

	insertDefaultFields(s)
	pgContainer, rdContainer, err := s.initContainers(ctx)
	if err != nil {
		t.Fatalf("init: %v", err)
	}

	pgHost, _ := pgContainer.Host(ctx)
	pgPort, _ := pgContainer.MappedPort(ctx, s.pgPort)
	pgAddr := fmt.Sprintf("%s:%s", pgHost, pgPort.Port())
	pgAdminDSN := fmt.Sprintf("postgres://postgres:%s@%s/%s?sslmode=disable", s.pgAdminPass, pgAddr, s.pgDBName)

	rdHost, _ := rdContainer.Host(ctx)
	rdPort, _ := rdContainer.MappedPort(ctx, s.rdPort)
	rdAddr := fmt.Sprintf("%s:%s", rdHost, rdPort.Port())

	rdb := redis.NewClient(&redis.Options{Addr: rdAddr, Password: s.rdPass, DB: s.rdDB})
	defer rdb.Close()
	pg, _ := pgx.Connect(ctx, pgAdminDSN)
	defer pg.Close(ctx)

	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	err = os.WriteFile(tmpFile, []byte(s.configYML), 0644)
	if err != nil {
		t.Fatalf("creating temp config: %v", err)
	}

	cfg, err := config.Load(tmpFile)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Redis.Addr = rdAddr
	cfg.Postgres.Addr = pgAddr

	if err := cfg.Validate(); err != nil {
		t.Fatalf("incorrect test config: %v", err)
	}

	rep, err := replicator.New(cfg)
	if err != nil {
		t.Fatalf("replicator init failed: %v", err)
	}

	// запуск репликатора в фоне
	repCtx, repCancel := context.WithCancel(ctx)

	repErrCh := make(chan error, 1)
	go func() {
		repErrCh <- rep.Run(repCtx)
	}()
	defer repCancel()

	// TODO: стоит ли давать сервису некоторое время для подключения?
	time.Sleep(2 * time.Second)

	emulateWork(ctx, t, pg, rdb)
	checkFunc(ctx, t, rdb)

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

// опрашивать redis до удаления ключа или таймаута
func waitForRedisKeys(ctx context.Context, t *testing.T, rdb *redis.Client, keys []string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		allMissing := true
		for _, key := range keys {
			exists, err := rdb.Exists(ctx, key).Result()
			if err != nil {
				t.Logf("redis check error for key %s: %v", key, err)
				allMissing = false
				break
			}
			if exists > 0 {
				allMissing = false
				break
			}
		}

		if allMissing {
			return true
		}

		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func TestCDC_CacheInvalidation(t *testing.T) {

	tests := []struct {
		name            string
		emulateWorkFunc func(ctx context.Context, t *testing.T, pgConn *pgx.Conn, rdConn *redis.Client)
		checkFunc       func(ctx context.Context, t *testing.T, rdConn *redis.Client)
	}{
		{
			name: "updating one record in a tracked table",
			emulateWorkFunc: func(ctx context.Context, t *testing.T, pgConn *pgx.Conn, rdConn *redis.Client) {
				// создание тестовой таблицы
				_, err := pgConn.Exec(ctx, `
				CREATE TABLE IF NOT EXISTS users (
					id SERIAL PRIMARY KEY,
					name TEXT NOT NULL,
					email TEXT NOT NULL
				);
			`)
				if err != nil {
					t.Fatalf("create table: %v", err)
				}

				// вставка строки
				_, err = pgConn.Exec(ctx, "INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com')")
				if err != nil {
					t.Errorf("insert user: %v", err)
				}

				// эмулируем кеш приложения
				// TODO: всегда ли ключ будет "user:1"?
				err = rdConn.Set(ctx, "user:1", `{"name":"Alice","email":"alice@test.com"}`, 0).Err()
				if err != nil {
					t.Errorf("set redis key: %v", err)
				}

				// изменяем данные в БД
				_, err = pgConn.Exec(ctx, "UPDATE users SET email='alice_updated@test.com' WHERE id=1")
				if err != nil {
					t.Errorf("update user: %v", err)
				}
			},
			checkFunc: func(ctx context.Context, t *testing.T, rdConn *redis.Client) {
				// так как CDC асинхронный нужно подождать
				deleted := waitForRedisKeys(ctx, t, rdConn, []string{"user:1"}, 1*time.Second)
				if !deleted {
					t.Fatal("Redis key 'user:1' was NOT deleted after UPDATE")
				}
			},
		},
		{
			name: "deleting one record in a tracked table",
			emulateWorkFunc: func(ctx context.Context, t *testing.T, pgConn *pgx.Conn, rdConn *redis.Client) {
				// создание тестовой таблицы
				_, err := pgConn.Exec(ctx, `
				CREATE TABLE IF NOT EXISTS users (
					id SERIAL PRIMARY KEY,
					name TEXT NOT NULL,
					email TEXT NOT NULL
				);
			`)
				if err != nil {
					t.Fatalf("create table: %v", err)
				}

				_, err = pgConn.Exec(ctx, "INSERT INTO users (name, email) VALUES ('Bob', 'bob@test.com')")
				if err != nil {
					t.Errorf("insert user: %v", err)
				}

				// TODO: проблема, всегда ли я буду запускать тесты в такой последовательности? если нет, то нужно как-то узнавать id для эмуляции добавления ключа
				// TODO: ну так сделать SELECT для надежности
				err = rdConn.Set(ctx, "user:1", `{"name":"Bob","email":"bob@test.com"}`, 0).Err()
				if err != nil {
					t.Errorf("set redis key: %v", err)
				}

				_, err = pgConn.Exec(ctx, "DELETE FROM users WHERE id=1")
				if err != nil {
					t.Errorf("delete user: %v", err)
				}
			},
			checkFunc: func(ctx context.Context, t *testing.T, rdConn *redis.Client) {
				deleted := waitForRedisKeys(ctx, t, rdConn, []string{"user:1"}, 1*time.Second)
				if !deleted {
					t.Fatal("Redis key 'user:1' was NOT deleted after UPDATE")
				}
			},
		},
		{
			name: "uncommitted changes",
			emulateWorkFunc: func(ctx context.Context, t *testing.T, pgConn *pgx.Conn, rdConn *redis.Client) {
				// создание тестовой таблицы
				_, err := pgConn.Exec(ctx, `
				CREATE TABLE IF NOT EXISTS users (
					id SERIAL PRIMARY KEY,
					name TEXT NOT NULL,
					email TEXT NOT NULL
				);
			`)
				if err != nil {
					t.Fatalf("create table: %v", err)
				}

				// вставка строки
				_, err = pgConn.Exec(ctx, "INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com')")
				if err != nil {
					t.Errorf("insert user: %v", err)
				}

				// эмулируем кеш приложения
				// TODO: всегда ли ключ будет "user:1"?
				err = rdConn.Set(ctx, "user:1", `{"name":"Alice","email":"alice@test.com"}`, 0).Err()
				if err != nil {
					t.Errorf("set redis key: %v", err)
				}

				// изменяем данные в БД
				_, err = pgConn.Exec(ctx, `
					BEGIN;
					UPDATE users SET email='alice_updated@test.com' WHERE id=1;
					ROLLBACK;
				`)
				if err != nil {
					t.Errorf("update user: %v", err)
				}
			},
			checkFunc: func(ctx context.Context, t *testing.T, rdConn *redis.Client) {
				// TODO: нужно сделать другой способ проверки, так как просто ждать и проверять ненадежно и долго
				t.Helper()

				key := "user:1"

				timeout := 1 * time.Second
				interval := 10 * time.Millisecond

				deadline := time.Now().Add(timeout)

				for time.Now().Before(deadline) {
					select {
					case <-ctx.Done():
						t.Errorf("Context cancelled before key '%s' was found: %v", key, ctx.Err())
						return
					default:
						exists, err := rdConn.Exists(ctx, key).Result()
						if err != nil {
							t.Errorf("Failed to check existence of key '%s': %v", key, err)
							return
						}

						if exists == 1 {
							t.Logf("Successfully verified that key '%s' exists in Redis", key)
							return
						}

						time.Sleep(interval)
					}
				}

				// Таймаут - ключ не появился
				t.Errorf("Timeout: key '%s' was not found in Redis after %v", key, timeout)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			initializer := ContainersConfig{}
			initializer.TestFunc(ctx, t, tt.emulateWorkFunc, tt.checkFunc)
		})
	}
}
