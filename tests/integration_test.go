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
	"github.com/Tkach360/tk_cdc/internal/service"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

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

	pgContainer testcontainers.Container
	rdContainer testcontainers.Container

	// функция очистки данных между тестами, отрабатывает в конце TestFunc
	clearFunc func(ctx context.Context, pgConn *pgx.Conn, rdConn *redis.Client) error
}

func NewContainersConfig(ctx context.Context) (*ContainersConfig, error) {
	cc := ContainersConfig{}
	insertDefaultFields(&cc)
	err := cc.initContainers(ctx)
	if err != nil {
		return nil, err
	}
	return &cc, nil
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

	if s.clearFunc == nil {
		s.clearFunc = func(ctx context.Context, pgConn *pgx.Conn, rdConn *redis.Client) error {
			return nil
		}
	}
}

func (s *ContainersConfig) initContainers(ctx context.Context) error {

	pgContainer, err := s.createPostgresContainer(ctx)
	if err != nil {
		return fmt.Errorf("creating postgres: %w", err)
	}

	pgHost, _ := pgContainer.Host(ctx)
	pgPort, _ := pgContainer.MappedPort(ctx, s.pgPort)
	pgAddr := fmt.Sprintf("%s:%s", pgHost, pgPort.Port())
	adminDSN := fmt.Sprintf("postgres://postgres:%s@%s/%s?sslmode=disable", s.pgAdminPass, pgAddr, s.pgDBName)

	err = s.setupDBForService(ctx, adminDSN)
	if err != nil {
		return fmt.Errorf("setup database: %w", err)
	}

	rdContainer, err := s.createRedisContainer(ctx)
	if err != nil {
		return fmt.Errorf("creating redis: %w", err)
	}

	s.pgContainer = pgContainer
	s.rdContainer = rdContainer

	return nil
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

	pgContainer := s.pgContainer
	rdContainer := s.rdContainer

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
	err := os.WriteFile(tmpFile, []byte(s.configYML), 0644)
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

	opts := &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: false,
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))
	service, err := service.New(cfg, logger)
	if err != nil {
		t.Fatalf("replicator init failed: %v", err)
	}

	// запуск репликатора в фоне
	repCtx, repCancel := context.WithCancel(ctx)

	repErrCh := make(chan error, 1)
	go func() {
		repErrCh <- service.Run(repCtx)
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

	if err := s.clearFunc(ctx, pg, rdb); err != nil {
		t.Fatalf("clearing after test: %v", err)
	}
}

func checkNoExistsKeys(t *testing.T, rdb *redis.Client, keys []string, timeout time.Duration) {
	assert.Eventually(t, func() bool {
		for _, key := range keys {
			exists, err := rdb.Exists(context.Background(), key).Result()
			if err != nil {
				t.Logf("Redis error: %v", err)
				return false
			}
			if exists > 0 {
				t.Logf("Key still exists: %s", key)
				return false
			}
		}
		return true
	}, timeout, 100*time.Millisecond, "Keys were not deleted within %v", timeout)
}

func checkExistsKeys(t *testing.T, rdb *redis.Client, keys []string, timeout time.Duration) {
	assert.Eventually(t, func() bool {
		for _, key := range keys {
			exists, err := rdb.Exists(context.Background(), key).Result()
			if err != nil {
				t.Logf("Redis error: %v", err)
				return false
			}
			if exists == 0 {
				t.Logf("Key not exists: %s", key)
				return true
			}
		}
		return true
	}, timeout, 100*time.Millisecond, "Keys were not deleted within %v", timeout)
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
				checkNoExistsKeys(t, rdConn, []string{"user:1"}, 1*time.Second)
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
				checkNoExistsKeys(t, rdConn, []string{"user:1"}, 1*time.Second)
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
				checkExistsKeys(t, rdConn, []string{"user:1"}, 2*time.Second)
			},
		},
		{
			name: "updating many records in a tracked table",
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

				_, err = pgConn.Exec(ctx, `
    INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@test.com'),
    ('Joe', 'cooljoe@test.com'),
    ('Bob', 'marley@test.com'),
    ('Skinny Pete', 'skinnypete@test.com'),
    ('Badger', 'badger@test.com')
`)
				if err != nil {
					t.Errorf("insert user: %v", err)
				}

				// эмулируем кеш приложения
				err = rdConn.MSet(ctx,
					"user:1", `{"name":"Alice","email":"alice@test.com"}`,
					"user:2", `{"name":"Joe","email":"cooljoe@test.com"}`,
					"user:3", `{"name":"Bob","email":"marley@test.com"}`,
					"user:4", `{"name":"Skinny Pete","email":"skinnypete@test.com"}`,
					"user:5", `{"name":"Badger","email":"badger@test.com"}`,
				).Err()
				if err != nil {
					t.Errorf("set redis key: %v", err)
				}

				// изменяем данные в БД
				_, err = pgConn.Exec(ctx, "UPDATE users SET email='alice_updated@test.com' WHERE id<10")
				if err != nil {
					t.Errorf("update user: %v", err)
				}
			},
			checkFunc: func(ctx context.Context, t *testing.T, rdConn *redis.Client) {
				checkNoExistsKeys(t, rdConn, []string{"user:1", "user:2", "user:3", "user:4", "user:5"}, 2*time.Second)
			},
		},
		{
			name: "multiple updates one without an update",
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

				_, err = pgConn.Exec(ctx, `
    INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@test.com'),
    ('Joe', 'cooljoe@test.com'),
    ('Bob', 'marley@test.com'),
    ('Skinny Pete', 'skinnypete@test.com'),
    ('Badger', 'badger@test.com'),
    ('Hank Schrader', 'schrader@test.com')
`)
				if err != nil {
					t.Errorf("insert user: %v", err)
				}

				// эмулируем кеш приложения
				err = rdConn.MSet(ctx,
					"user:1", `{"name":"Alice","email":"alice@test.com"}`,
					"user:2", `{"name":"Joe","email":"cooljoe@test.com"}`,
					"user:3", `{"name":"Bob","email":"marley@test.com"}`,
					"user:4", `{"name":"Skinny Pete","email":"skinnypete@test.com"}`,
					"user:5", `{"name":"Badger","email":"badger@test.com"}`,
					"user:6", `{"name":"Hank Schrader","email":"schrader@test.com"}`,
				).Err()
				if err != nil {
					t.Errorf("set redis key: %v", err)
				}

				// изменяем данные в БД
				_, err = pgConn.Exec(ctx, "UPDATE users SET email='updated@test.com' WHERE id<6")
				if err != nil {
					t.Errorf("update user: %v", err)
				}
			},
			checkFunc: func(ctx context.Context, t *testing.T, rdConn *redis.Client) {
				checkNoExistsKeys(t, rdConn, []string{"user:1", "user:2", "user:3", "user:4", "user:5"}, 2*time.Second)
				checkExistsKeys(t, rdConn, []string{"user:6"}, 2*time.Second)
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	initializer, err := NewContainersConfig(ctx)
	if err != nil {
		t.Fatalf("Containers config: %v", err)
	}
	initializer.clearFunc = func(ctx context.Context, pgConn *pgx.Conn, rdConn *redis.Client) error {
		// удаляю таблицу, чтобы тесты не влияли друг на друга
		_, err = pgConn.Exec(ctx, "DROP TABLE users;")
		if err != nil {
			t.Fatalf("drop table: %v", err)
		}
		if err != nil {
			return err
		}

		// также очищаю redis
		return rdConn.FlushAll(ctx).Err()
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initializer.TestFunc(ctx, t, tt.emulateWorkFunc, tt.checkFunc)
		})
	}
}
