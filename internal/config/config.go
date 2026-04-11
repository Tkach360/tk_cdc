// internal/config/config.go

// логика чтения конфигурации сервиса
package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/Tkach360/tk_cdc/internal/mapper"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Postgres PostgresConfig       `yaml:"postgres"`
	Redis    RedisConfig          `yaml:"redis"`
	Mapping  mapper.MappingConfig `yaml:"mapping"`
}

type PostgresConfig struct {
	DSN             string `yaml:"dsn"`
	ReplicationSlot string `yaml:"replication_slot"`
	Plugin          string `yaml:"plugin"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	expanded := os.ExpandEnv(string(data))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), cfg); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) validate() error {
	if c.Postgres.DSN == "" {
		return ErrPostgresDSNRequired
	}
	if c.Postgres.ReplicationSlot == "" {
		return ErrPostgresReplicationSlotRequired
	}
	if c.Postgres.Plugin != "pgoutput" {
		return ErrPostgresPluginUnsupported
	}

	if c.Redis.Addr == "" {
		return ErrRedisAddrRequired
	}

	for i, rule := range c.Mapping.Rules {
		if rule.Table.Name == "" {
			return &MappingError{i, ErrMappingTableRequired}
		}
		if rule.KeyPattern == "" {
			return &MappingError{Index: i, Err: ErrMappingKeyPatternRequired}
		}
		// TODO: нужно сделать более доскональную проверку формата поля и парсить в какую-нибудь структуру
		if !strings.Contains(rule.KeyPattern, "{") {
			return &MappingError{i, ErrMappingKeyPatternNoPlaceholder}
		}
	}

	return nil
}
