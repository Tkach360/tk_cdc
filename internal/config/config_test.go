package config

import (
	"errors"
	"testing"

	"github.com/Tkach360/tk_cdc/internal/mapper"
	"github.com/stretchr/testify/assert"
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
				Mapping: []mapper.MappingRule{
					{
						Table:      mapper.Table{Name: ""},
						KeyPattern: "user:{id}",
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
				Mapping: []mapper.MappingRule{
					{
						Table:      mapper.Table{Name: "users"},
						KeyPattern: "",
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
				Mapping: []mapper.MappingRule{
					{
						Table:      mapper.Table{Name: "users"},
						KeyPattern: "static_key",
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
				Mapping: []mapper.MappingRule{
					{
						Table:      mapper.Table{Name: "users"},
						KeyPattern: "user:{id}",
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
