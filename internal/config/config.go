// internal/config/config.go

// логика чтения конфигурации сервиса
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Tkach360/tk_cdc/internal/mapper"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Postgres PostgresConfig       `yaml:"postgres"`
	Redis    RedisConfig          `yaml:"redis"`
	Mapping  []mapper.MappingRule `yaml:"mapping"`
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

	// TODO: улучшить работу с ошибками для тестов
	if c.Postgres.DSN == "" {
		return errors.New("postgres.dsn is required")
	}
	if c.Postgres.ReplicationSlot == "" {
		return errors.New("postgres.replication_slot is required")
	}
	if c.Postgres.Plugin != "pgoutput" {
		return errors.New("only 'pgoutput' plugin is supported")
	}
	if c.Redis.Addr == "" {
		return errors.New("redis.addr is required")
	}
	for i, rule := range c.Mapping {
		if rule.Table.Name == "" {
			return fmt.Errorf("mapping[%d]: table is required", i)
		}
		if rule.KeyPattern == "" {
			return fmt.Errorf("mapping[%d]: key_pattern is required", i)
		}
		// TODO: нужно сделать более доскональную проверку формата поля и парсить в какую-нибудь структуру
		if !strings.Contains(rule.KeyPattern, "{") {
			return fmt.Errorf("mapping[%d]: key_pattern must contain {field} placeholder", i)
		}
	}
	return nil
}
