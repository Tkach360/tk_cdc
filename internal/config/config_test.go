package config

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Tkach360/tk_cdc/internal/mapper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expectedErr error
		checkErr    func(error) bool
	}{
		{
			name: "missing postgres addr",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       10,
				},
			},
			checkErr: func(err error) bool {
				var e *RequiredError
				return errors.As(err, &e)
			},
		},
		{
			name: "missing postgres db",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       10,
				},
			},
			checkErr: func(err error) bool {
				var e *RequiredError
				return errors.As(err, &e)
			},
		},
		{
			name: "missing replication user",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       10,
				},
			},
			checkErr: func(err error) bool {
				var e *RequiredError
				return errors.As(err, &e)
			},
		},
		{
			name: "missing replication password",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       10,
				},
			},
			checkErr: func(err error) bool {
				var e *RequiredError
				return errors.As(err, &e)
			},
		},
		{
			name: "missing app user",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       10,
				},
			},
			checkErr: func(err error) bool {
				var e *RequiredError
				return errors.As(err, &e)
			},
		},
		{
			name: "missing app password",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       10,
				},
			},
			checkErr: func(err error) bool {
				var e *RequiredError
				return errors.As(err, &e)
			},
		},
		{
			name: "missing replication slot",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "",
					Plugin:            "pgoutput",
					PublicationNames:  "",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       10,
				},
			},
			checkErr: func(err error) bool {
				var e *RequiredError
				return errors.As(err, &e)
			},
		},
		{
			name: "negative reconnect_max_attempts",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "my_publication",
					ReconnMaxAttempts: -1,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       10,
				},
			},
			checkErr: func(err error) bool {
				var e *InvalidValueError
				return errors.As(err, &e)
			},
		},
		{
			name: "negative reconnect_delay_ms",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "my_publication",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     -100,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       10,
				},
			},
			checkErr: func(err error) bool {
				var e *InvalidValueError
				return errors.As(err, &e)
			},
		},
		{
			name: "zero reconnect_delay_ms (valid, will use default)",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "my_publication",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     0,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       10,
				},
			},
			checkErr: func(err error) bool {
				// 0 is valid and will be set to default in setDefaults()
				return err == nil
			},
		},
		{
			name: "unsupported plugin",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "wal2json",
					PublicationNames:  "",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       10,
				},
			},
			checkErr: func(err error) bool {
				return errors.Is(err, ErrPostgresPluginUnsupported)
			},
		},
		{
			name: "missing redis addr",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "my_publication",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "",
					QMaxAttempts: 3,
					QDelay:       10,
				},
			},
			checkErr: func(err error) bool {
				var e *RequiredError
				return errors.As(err, &e)
			},
		},
		{
			name: "negative query_max_attempts",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "my_publication",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: -1,
					QDelay:       10,
				},
			},
			checkErr: func(err error) bool {
				var e *InvalidValueError
				return errors.As(err, &e)
			},
		},
		{
			name: "negative query_delay_ms",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "my_publication",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       -5,
				},
			},
			checkErr: func(err error) bool {
				var e *InvalidValueError
				return errors.As(err, &e)
			},
		},
		{
			name: "zero query_max_attempts",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "my_publication",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 0,
					QDelay:       10,
				},
			},
			checkErr: func(err error) bool {
				var e *InvalidValueError
				return errors.As(err, &e)
			},
		},
		{
			name: "query_max_attempts = 1",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "my_publication",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 1,
					QDelay:       10,
				},
			},
			checkErr: func(err error) bool {
				return err == nil
			},
		},
		{
			name: "zero query_delay_ms",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "my_publication",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       0,
				},
			},
			checkErr: func(err error) bool {
				return err == nil
			},
		},
		{
			name: "missing publication names with pgoutput",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       10,
				},
			},
			checkErr: func(err error) bool {
				var e *RequiredError
				return errors.As(err, &e)
			},
		},
		{
			name: "missing table name in mapping",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "my_publication",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       10,
				},
				Mapping: mapper.MappingConfig{
					DefaultSchema: "public",
					Rules: []mapper.MappingRule{
						{
							Table:      mapper.Table{Name: ""},
							KeyPattern: "user:{id}",
						},
					},
				},
			},
			checkErr: func(err error) bool {
				if err == nil {
					return false
				}
				var mappingErr *MappingError
				if errors.As(err, &mappingErr) {
					return errors.Is(mappingErr.Err, ErrMappingTableRequired) && mappingErr.Index == 0
				}
				return false
			},
		},
		{
			name: "missing key pattern",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "my_publication",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       10,
				},
				Mapping: mapper.MappingConfig{
					DefaultSchema: "public",
					Rules: []mapper.MappingRule{
						{
							Table:      mapper.Table{Name: "users"},
							KeyPattern: "",
						},
					},
				},
			},
			checkErr: func(err error) bool {
				var mappingErr *MappingError
				if errors.As(err, &mappingErr) {
					return errors.Is(mappingErr.Err, ErrMappingKeyPatternRequired) && mappingErr.Index == 0
				}
				return false
			},
		},
		{
			name: "key pattern without placeholder",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "my_publication",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       10,
				},
				Mapping: mapper.MappingConfig{
					DefaultSchema: "public",
					Rules: []mapper.MappingRule{
						{
							Table:      mapper.Table{Name: "users"},
							KeyPattern: "static_key",
						},
					},
				},
			},
			checkErr: func(err error) bool {
				var mappingErr *MappingError
				if errors.As(err, &mappingErr) {
					return errors.Is(mappingErr.Err, ErrMappingKeyPatternNoPlaceholder) && mappingErr.Index == 0
				}
				return false
			},
		},
		{
			name: "valid config with all fields including reconnect",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "my_publication",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 5,
					QDelay:       100,
				},
				Mapping: mapper.MappingConfig{
					DefaultSchema: "public",
					Rules: []mapper.MappingRule{
						{
							Table:      mapper.Table{Name: "users"},
							KeyPattern: "user:{id}",
						},
					},
				},
			},
			checkErr: func(err error) bool {
				return err == nil
			},
		},
		{
			name: "valid config with multiple publications",
			config: &Config{
				Postgres: PostgresConfig{
					Addr:              "localhost:5432",
					DB:                "testdb",
					ReplicationUser:   "repl_user",
					ReplicationPass:   "repl_pass",
					AppUser:           "app_user",
					AppPass:           "app_pass",
					ReplicationSlot:   "test_slot",
					Plugin:            "pgoutput",
					PublicationNames:  "pub1,pub2,pub3",
					ReconnMaxAttempts: 5,
					ReconnDelayMs:     1000,
				},
				Redis: RedisConfig{
					Addr:         "localhost:6379",
					QMaxAttempts: 3,
					QDelay:       10,
				},
				Mapping: mapper.MappingConfig{
					DefaultSchema: "public",
					Rules: []mapper.MappingRule{
						{
							Table:      mapper.Table{Name: "users"},
							KeyPattern: "user:{id}",
						},
					},
				},
			},
			checkErr: func(err error) bool {
				return err == nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.validate()
			assert.True(t, tt.checkErr(err), "Expected error condition not met, got: %v", err)
		})
	}
}

func createTempConfig(t *testing.T, content string) string {
	t.Helper()
	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(tmpFile, []byte(content), 0644)
	require.NoError(t, err)
	return tmpFile
}

func TestLoad(t *testing.T) {
	tests := []struct {
		name        string
		yamlContent string
		env         map[string]string
		wantErr     bool
		checkFunc   func(t *testing.T, cfg *Config)
	}{
		{
			name: "valid config with default_schema and rule without schema",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "my_pub"
  reconnect_max_attempts: 5
  reconnect_delay_ms: 1000
redis:
  addr: "localhost:6379"
  query_max_attempts: 3
  query_delay_ms: 10
mapping:
  default_schema: "custom"
  rules:
    - table: "users"
      key_pattern: "user:{id}"
`,
			wantErr: false,
			checkFunc: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "localhost:5432", cfg.Postgres.Addr)
				assert.Equal(t, "testdb", cfg.Postgres.DB)
				assert.Equal(t, "repl_user", cfg.Postgres.ReplicationUser)
				assert.Equal(t, "repl_pass", cfg.Postgres.ReplicationPass)
				assert.Equal(t, "app_user", cfg.Postgres.AppUser)
				assert.Equal(t, "app_pass", cfg.Postgres.AppPass)
				assert.Equal(t, 5, cfg.Postgres.ReconnMaxAttempts)
				assert.Equal(t, 1000, cfg.Postgres.ReconnDelayMs)
				assert.Equal(t, "custom", cfg.Mapping.DefaultSchema)
				require.Len(t, cfg.Mapping.Rules, 1)
				rule := cfg.Mapping.Rules[0]
				assert.Equal(t, "custom", rule.Table.Schema)
				assert.Equal(t, "users", rule.Table.Name)
				assert.Equal(t, "user:{id}", rule.KeyPattern)
				assert.Equal(t, "my_pub", cfg.Postgres.PublicationNames)
				assert.Equal(t, 3, cfg.Redis.QMaxAttempts)
				assert.Equal(t, 10, cfg.Redis.QDelay)
			},
		},
		{
			name: "no default_schema - fallback to public",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  reconnect_max_attempts: 3
  reconnect_delay_ms: 5000
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "orders_pub"
redis:
  addr: "localhost:6379"
  query_max_attempts: 5
  query_delay_ms: 50
mapping:
  rules:
    - table: "orders"
      key_pattern: "order:{id}"
`,
			wantErr: false,
			checkFunc: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "public", cfg.Mapping.DefaultSchema)
				rule := cfg.Mapping.Rules[0]
				assert.Equal(t, "public", rule.Table.Schema)
				assert.Equal(t, "orders", rule.Table.Name)
				assert.Equal(t, "orders_pub", cfg.Postgres.PublicationNames)
				assert.Equal(t, 5, cfg.Redis.QMaxAttempts)
				assert.Equal(t, 50, cfg.Redis.QDelay)
			},
		},
		{
			name: "explicit schema in table - no substitution",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  reconnect_max_attempts: 3
  reconnect_delay_ms: 5000
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "test_pub"
redis:
  addr: "localhost:6379"
  query_max_attempts: 7
  query_delay_ms: 200
mapping:
  default_schema: "custom"
  rules:
    - table: "explicit.users"
      key_pattern: "user:{id}"
`,
			wantErr: false,
			checkFunc: func(t *testing.T, cfg *Config) {
				rule := cfg.Mapping.Rules[0]
				assert.Equal(t, "explicit", rule.Table.Schema)
				assert.Equal(t, "users", rule.Table.Name)
				assert.Equal(t, "test_pub", cfg.Postgres.PublicationNames)
				assert.Equal(t, 7, cfg.Redis.QMaxAttempts)
				assert.Equal(t, 200, cfg.Redis.QDelay)
			},
		},
		{
			name: "empty schema in table ('.table') - uses default_schema",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  reconnect_max_attempts: 3
  reconnect_delay_ms: 5000
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "temp_pub"
redis:
  addr: "localhost:6379"
  query_max_attempts: 3
  query_delay_ms: 100
mapping:
  default_schema: "custom"
  rules:
    - table: ".temp"
      key_pattern: "temp:{id}"
`,
			wantErr: false,
			checkFunc: func(t *testing.T, cfg *Config) {
				rule := cfg.Mapping.Rules[0]
				assert.Equal(t, "custom", rule.Table.Schema)
				assert.Equal(t, "temp", rule.Table.Name)
				assert.Equal(t, "temp_pub", cfg.Postgres.PublicationNames)
				assert.Equal(t, 3, cfg.Redis.QMaxAttempts)
				assert.Equal(t, 100, cfg.Redis.QDelay)
			},
		},
		{
			name: "missing required postgres addr",
			yamlContent: `
postgres:
  db: "testdb"
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "my_pub"
redis:
  addr: "localhost:6379"
  query_max_attempts: 3
  query_delay_ms: 10
mapping:
  rules:
    - table: "users"
      key_pattern: "user:{id}"
`,
			wantErr: true,
		},
		{
			name: "missing required postgres db",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "my_pub"
redis:
  addr: "localhost:6379"
  query_max_attempts: 3
  query_delay_ms: 10
mapping:
  rules:
    - table: "users"
      key_pattern: "user:{id}"
`,
			wantErr: true,
		},
		{
			name: "missing replication_user",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "my_pub"
redis:
  addr: "localhost:6379"
  query_max_attempts: 3
  query_delay_ms: 10
mapping:
  rules:
    - table: "users"
      key_pattern: "user:{id}"
`,
			wantErr: true,
		},
		{
			name: "missing redis.addr",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "my_pub"
redis:
  db: 0
  query_max_attempts: 3
  query_delay_ms: 10
mapping:
  rules:
    - table: "users"
      key_pattern: "user:{id}"
`,
			wantErr: true,
		},
		{
			name: "invalid plugin (not pgoutput)",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "wal2json"
  publication_names: "my_pub"
redis:
  addr: "localhost:6379"
  query_max_attempts: 3
  query_delay_ms: 10
mapping:
  rules:
    - table: "users"
      key_pattern: "user:{id}"
`,
			wantErr: true,
		},
		{
			name: "missing publication_names with pgoutput",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
redis:
  addr: "localhost:6379"
  query_max_attempts: 3
  query_delay_ms: 10
mapping:
  rules:
    - table: "users"
      key_pattern: "user:{id}"
`,
			wantErr: true,
		},
		{
			name: "empty table name in rule",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "my_pub"
redis:
  addr: "localhost:6379"
  query_max_attempts: 3
  query_delay_ms: 10
mapping:
  rules:
    - table: ""
      key_pattern: "user:{id}"
`,
			wantErr: true,
		},
		{
			name: "empty key_pattern",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "my_pub"
redis:
  addr: "localhost:6379"
  query_max_attempts: 3
  query_delay_ms: 10
mapping:
  rules:
    - table: "users"
      key_pattern: ""
`,
			wantErr: true,
		},
		{
			name: "key_pattern missing placeholder {}",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "my_pub"
redis:
  addr: "localhost:6379"
  query_max_attempts: 3
  query_delay_ms: 10
mapping:
  rules:
    - table: "users"
      key_pattern: "user:id"
`,
			wantErr: true,
		},
		{
			name: "environment variable expansion",
			yamlContent: `
postgres:
  addr: "${POSTGRES_ADDR}"
  db: "${POSTGRES_DB}"
  replication_user: "${PG_REPL_USER}"
  replication_pass: "${PG_REPL_PASS}"
  app_user: "${PG_APP_USER}"
  app_pass: "${PG_APP_PASS}"
  replication_slot: "${PG_SLOT}"
  plugin: "pgoutput"
  publication_names: "${PG_PUBLICATIONS}"
  reconnect_max_attempts: ${RECONN_MAX_ATTEMPTS}
  reconnect_delay_ms: ${RECONN_DELAY_MS}
redis:
  addr: "${REDIS_ADDR}"
  query_max_attempts: ${REDIS_MAX_ATTEMPTS}
  query_delay_ms: ${REDIS_DELAY_MS}
mapping:
  rules:
    - table: "users"
      key_pattern: "user:{id}"
`,
			env: map[string]string{
				"POSTGRES_ADDR":       "localhost:5432",
				"POSTGRES_DB":         "testdb",
				"PG_REPL_USER":        "repl_user",
				"PG_REPL_PASS":        "repl_pass",
				"PG_APP_USER":         "app_user",
				"PG_APP_PASS":         "app_pass",
				"PG_SLOT":             "test_slot",
				"REDIS_ADDR":          "redis:6379",
				"PG_PUBLICATIONS":     "env_pub1,env_pub2",
				"REDIS_MAX_ATTEMPTS":  "5",
				"REDIS_DELAY_MS":      "150",
				"RECONN_MAX_ATTEMPTS": "7",
				"RECONN_DELAY_MS":     "3000",
			},
			wantErr: false,
			checkFunc: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "localhost:5432", cfg.Postgres.Addr)
				assert.Equal(t, "testdb", cfg.Postgres.DB)
				assert.Equal(t, "repl_user", cfg.Postgres.ReplicationUser)
				assert.Equal(t, "repl_pass", cfg.Postgres.ReplicationPass)
				assert.Equal(t, "app_user", cfg.Postgres.AppUser)
				assert.Equal(t, "app_pass", cfg.Postgres.AppPass)
				assert.Equal(t, "test_slot", cfg.Postgres.ReplicationSlot)
				assert.Equal(t, "redis:6379", cfg.Redis.Addr)
				assert.Equal(t, "env_pub1,env_pub2", cfg.Postgres.PublicationNames)
				assert.Equal(t, 7, cfg.Postgres.ReconnMaxAttempts)
				assert.Equal(t, 3000, cfg.Postgres.ReconnDelayMs)
				assert.Equal(t, 5, cfg.Redis.QMaxAttempts)
				assert.Equal(t, 150, cfg.Redis.QDelay)
			},
		},
		{
			name: "multiple rules with mixed schema definitions",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  reconnect_max_attempts: 3
  reconnect_delay_ms: 5000
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "multi_pub"
redis:
  addr: "localhost:6379"
  query_max_attempts: 3
  query_delay_ms: 10
mapping:
  default_schema: "custom"
  rules:
    - table: "users"
      key_pattern: "user:{id}"
    - table: "explicit.orders"
      key_pattern: "order:{order_id}"
    - table: ".temp"
      key_pattern: "temp:{id}"
    - table: "analytics.events"
      key_pattern: "event:{event_id}"
`,
			wantErr: false,
			checkFunc: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "custom", cfg.Mapping.DefaultSchema)
				require.Len(t, cfg.Mapping.Rules, 4)

				assert.Equal(t, "custom", cfg.Mapping.Rules[0].Table.Schema)
				assert.Equal(t, "users", cfg.Mapping.Rules[0].Table.Name)
				assert.Equal(t, "user:{id}", cfg.Mapping.Rules[0].KeyPattern)

				assert.Equal(t, "explicit", cfg.Mapping.Rules[1].Table.Schema)
				assert.Equal(t, "orders", cfg.Mapping.Rules[1].Table.Name)
				assert.Equal(t, "order:{order_id}", cfg.Mapping.Rules[1].KeyPattern)

				assert.Equal(t, "custom", cfg.Mapping.Rules[2].Table.Schema)
				assert.Equal(t, "temp", cfg.Mapping.Rules[2].Table.Name)
				assert.Equal(t, "temp:{id}", cfg.Mapping.Rules[2].KeyPattern)

				assert.Equal(t, "analytics", cfg.Mapping.Rules[3].Table.Schema)
				assert.Equal(t, "events", cfg.Mapping.Rules[3].Table.Name)
				assert.Equal(t, "event:{event_id}", cfg.Mapping.Rules[3].KeyPattern)

				assert.Equal(t, "multi_pub", cfg.Postgres.PublicationNames)
				assert.Equal(t, 3, cfg.Redis.QMaxAttempts)
				assert.Equal(t, 10, cfg.Redis.QDelay)
			},
		},
		{
			name: "no rules defined",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  reconnect_max_attempts: 3
  reconnect_delay_ms: 5000
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "empty_rules_pub"
redis:
  addr: "localhost:6379"
  query_max_attempts: 3
  query_delay_ms: 10
mapping:
  default_schema: "public"
  rules: []
`,
			wantErr: false,
			checkFunc: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "public", cfg.Mapping.DefaultSchema)
				assert.Empty(t, cfg.Mapping.Rules)
				assert.Equal(t, "empty_rules_pub", cfg.Postgres.PublicationNames)
				assert.Equal(t, 3, cfg.Redis.QMaxAttempts)
				assert.Equal(t, 10, cfg.Redis.QDelay)
			},
		},
		{
			name: "no rules and no default_schema",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  reconnect_max_attempts: 3
  reconnect_delay_ms: 5000
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "minimal_pub"
redis:
  addr: "localhost:6379"
  query_max_attempts: 3
  query_delay_ms: 10
mapping:
  rules: []
`,
			wantErr: false,
			checkFunc: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "public", cfg.Mapping.DefaultSchema)
				assert.Empty(t, cfg.Mapping.Rules)
				assert.Equal(t, "minimal_pub", cfg.Postgres.PublicationNames)
				assert.Equal(t, 3, cfg.Redis.QMaxAttempts)
				assert.Equal(t, 10, cfg.Redis.QDelay)
			},
		},
		{
			name: "negative reconnect_max_attempts in yaml",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  reconnect_max_attempts: 3
  reconnect_delay_ms: 5000
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "my_pub"
  reconnect_max_attempts: -5
  reconnect_delay_ms: 1000
redis:
  addr: "localhost:6379"
  query_max_attempts: 3
  query_delay_ms: 10
mapping:
  default_schema: "custom"
  rules:
    - table: "users"
      key_pattern: "user:{id}"
`,
			wantErr: true,
		},
		{
			name: "negative reconnect_delay_ms in yaml",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "my_pub"
  reconnect_max_attempts: 5
  reconnect_delay_ms: -1000
redis:
  addr: "localhost:6379"
  query_max_attempts: 3
  query_delay_ms: 10
mapping:
  default_schema: "custom"
  rules:
    - table: "users"
      key_pattern: "user:{id}"
`,
			wantErr: true,
		},
		{
			name: "negative query_max_attempts in yaml",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  reconnect_max_attempts: 3
  reconnect_delay_ms: 5000
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "my_pub"
redis:
  addr: "localhost:6379"
  query_max_attempts: -3
  query_delay_ms: 10
mapping:
  default_schema: "custom"
  rules:
    - table: "users"
      key_pattern: "user:{id}"
`,
			wantErr: true,
		},
		{
			name: "negative query_delay_ms in yaml",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "my_pub"
redis:
  addr: "localhost:6379"
  query_max_attempts: 3
  query_delay_ms: -10
mapping:
  default_schema: "custom"
  rules:
    - table: "users"
      key_pattern: "user:{id}"
`,
			wantErr: true,
		},
		{
			name: "custom retrying invalidation values",
			yamlContent: `
postgres:
  addr: "localhost:5432"
  db: "testdb"
  replication_user: "repl_user"
  replication_pass: "repl_pass"
  app_user: "app_user"
  app_pass: "app_pass"
  replication_slot: "slot1"
  plugin: "pgoutput"
  publication_names: "my_pub"
  reconnect_max_attempts: 15
  reconnect_delay_ms: 5000
redis:
  addr: "localhost:6379"
  query_max_attempts: 10
  query_delay_ms: 500
mapping:
  default_schema: "custom"
  rules:
    - table: "users"
      key_pattern: "user:{id}"
`,
			wantErr: false,
			checkFunc: func(t *testing.T, cfg *Config) {
				assert.Equal(t, 15, cfg.Postgres.ReconnMaxAttempts)
				assert.Equal(t, 5000, cfg.Postgres.ReconnDelayMs)
				assert.Equal(t, 10, cfg.Redis.QMaxAttempts)
				assert.Equal(t, 500, cfg.Redis.QDelay)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				os.Setenv(k, v)
				defer os.Unsetenv(k)
			}
			cfgPath := createTempConfig(t, tt.yamlContent)
			cfg, err := Load(cfgPath)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			if tt.checkFunc != nil {
				tt.checkFunc(t, cfg)
			}
		})
	}
}
