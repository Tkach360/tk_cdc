// internal/config/config.go

// логика чтения конфигурации сервиса
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Postgres PostgresConfig `yaml:"postgres"`
	Redis    RedisConfig    `yaml:"redis"`
	Mapping  []MappingRule  `yaml:"mapping"`
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

type MappingRule struct {
	Table      string `yaml:"table"`
	KeyPattern string `yaml:"key_pattern"`
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

func (c Config) validate() error {
	// TODO: сделать метод Config.validate
	return nil
}
