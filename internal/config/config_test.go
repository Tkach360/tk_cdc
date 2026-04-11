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
			name: "missing postgres dsn",
			config: &Config{
				Postgres: PostgresConfig{
					DSN:             "",
					ReplicationSlot: "test_slot",
					Plugin:          "pgoutput",
				},
				Redis: RedisConfig{
					Addr: "localhost:6379",
				},
			},
			checkErr: func(err error) bool {
				return errors.Is(err, ErrPostgresDSNRequired)
			},
		},
		{
			name: "missing replication slot",
			config: &Config{
				Postgres: PostgresConfig{
					DSN:             "postgres://localhost:5432/db",
					ReplicationSlot: "",
					Plugin:          "pgoutput",
				},
				Redis: RedisConfig{
					Addr: "localhost:6379",
				},
			},
			checkErr: func(err error) bool {
				return errors.Is(err, ErrPostgresReplicationSlotRequired)
			},
		},
		{
			name: "unsupported plugin",
			config: &Config{
				Postgres: PostgresConfig{
					DSN:             "postgres://localhost:5432/db",
					ReplicationSlot: "test_slot",
					Plugin:          "wal2json",
				},
				Redis: RedisConfig{
					Addr: "localhost:6379",
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
					DSN:             "postgres://localhost:5432/db",
					ReplicationSlot: "test_slot",
					Plugin:          "pgoutput",
				},
				Redis: RedisConfig{
					Addr: "",
				},
			},
			checkErr: func(err error) bool {
				return errors.Is(err, ErrRedisAddrRequired)
			},
		},
		{
			name: "missing table name in mapping",
			config: &Config{
				Postgres: PostgresConfig{
					DSN:             "postgres://localhost:5432/db",
					ReplicationSlot: "test_slot",
					Plugin:          "pgoutput",
				},
				Redis: RedisConfig{
					Addr: "localhost:6379",
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
					DSN:             "postgres://localhost:5432/db",
					ReplicationSlot: "test_slot",
					Plugin:          "pgoutput",
				},
				Redis: RedisConfig{
					Addr: "localhost:6379",
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
					DSN:             "postgres://localhost:5432/db",
					ReplicationSlot: "test_slot",
					Plugin:          "pgoutput",
				},
				Redis: RedisConfig{
					Addr: "localhost:6379",
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
			name: "valid config",
			config: &Config{
				Postgres: PostgresConfig{
					DSN:             "postgres://localhost:5432/db",
					ReplicationSlot: "test_slot",
					Plugin:          "pgoutput",
				},
				Redis: RedisConfig{
					Addr: "localhost:6379",
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
  dsn: "postgres://localhost:5432/db"
  replication_slot: "slot1"
  plugin: "pgoutput"
redis:
  addr: "localhost:6379"
mapping:
  default_schema: "custom"
  rules:
    - table: "users"
      key_pattern: "user:{id}"
`,
			wantErr: false,
			checkFunc: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "custom", cfg.Mapping.DefaultSchema)
				require.Len(t, cfg.Mapping.Rules, 1)
				rule := cfg.Mapping.Rules[0]
				assert.Equal(t, "custom", rule.Table.Schema)
				assert.Equal(t, "users", rule.Table.Name)
				assert.Equal(t, "user:{id}", rule.KeyPattern)
			},
		},
		{
			name: "no default_schema - fallback to public",
			yamlContent: `
postgres:
  dsn: "postgres://localhost:5432/db"
  replication_slot: "slot1"
  plugin: "pgoutput"
redis:
  addr: "localhost:6379"
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
			},
		},
		{
			name: "explicit schema in table - no substitution",
			yamlContent: `
postgres:
  dsn: "postgres://localhost:5432/db"
  replication_slot: "slot1"
  plugin: "pgoutput"
redis:
  addr: "localhost:6379"
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
			},
		},
		{
			name: "empty schema in table ('.table') - uses default_schema",
			yamlContent: `
postgres:
  dsn: "postgres://localhost:5432/db"
  replication_slot: "slot1"
  plugin: "pgoutput"
redis:
  addr: "localhost:6379"
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
			},
		},
		{
			name: "missing required postgres.dsn",
			yamlContent: `
postgres:
  replication_slot: "slot1"
  plugin: "pgoutput"
redis:
  addr: "localhost:6379"
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
  dsn: "postgres://localhost:5432/db"
  replication_slot: "slot1"
  plugin: "pgoutput"
redis:
  db: 0
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
  dsn: "postgres://localhost:5432/db"
  replication_slot: "slot1"
  plugin: "wal2json"
redis:
  addr: "localhost:6379"
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
  dsn: "postgres://localhost:5432/db"
  replication_slot: "slot1"
  plugin: "pgoutput"
redis:
  addr: "localhost:6379"
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
  dsn: "postgres://localhost:5432/db"
  replication_slot: "slot1"
  plugin: "pgoutput"
redis:
  addr: "localhost:6379"
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
  dsn: "postgres://localhost:5432/db"
  replication_slot: "slot1"
  plugin: "pgoutput"
redis:
  addr: "localhost:6379"
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
  dsn: "${POSTGRES_DSN}"
  replication_slot: "${PG_SLOT}"
  plugin: "pgoutput"
redis:
  addr: "${REDIS_ADDR}"
mapping:
  rules:
    - table: "users"
      key_pattern: "user:{id}"
`,
			env: map[string]string{
				"POSTGRES_DSN": "postgres://test:pass@localhost:5432/testdb",
				"PG_SLOT":      "test_slot",
				"REDIS_ADDR":   "redis:6379",
			},
			wantErr: false,
			checkFunc: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "postgres://test:pass@localhost:5432/testdb", cfg.Postgres.DSN)
				assert.Equal(t, "test_slot", cfg.Postgres.ReplicationSlot)
				assert.Equal(t, "redis:6379", cfg.Redis.Addr)
			},
		},
		{
			name: "multiple rules with mixed schema definitions",
			yamlContent: `
postgres:
  dsn: "postgres://localhost:5432/db"
  replication_slot: "slot1"
  plugin: "pgoutput"
redis:
  addr: "localhost:6379"
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
			},
		},
		{
			name: "no rules defined",
			yamlContent: `
postgres:
  dsn: "postgres://localhost:5432/db"
  replication_slot: "slot1"
  plugin: "pgoutput"
redis:
  addr: "localhost:6379"
mapping:
  default_schema: "public"
  rules: []
`,
			wantErr: false,
			checkFunc: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "public", cfg.Mapping.DefaultSchema)
				assert.Empty(t, cfg.Mapping.Rules)
			},
		},
		{
			name: "no rules and no default_schema",
			yamlContent: `
postgres:
  dsn: "postgres://localhost:5432/db"
  replication_slot: "slot1"
  plugin: "pgoutput"
redis:
  addr: "localhost:6379"
mapping:
  rules: []
`,
			wantErr: false,
			checkFunc: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "public", cfg.Mapping.DefaultSchema)
				assert.Empty(t, cfg.Mapping.Rules)
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
