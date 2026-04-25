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
	Addr string `yaml:"addr"`
	DB   string `yaml:"db"`

	ReconnMaxAttempts int `yaml:"reconnect_max_attempts"`
	ReconnDelayMs     int `yaml:"reconnect_delay_ms"`

	ReplicationUser string `yaml:"replication_user"`
	ReplicationPass string `yaml:"replication_pass"`

	AppUser string `yaml:"app_user"`
	AppPass string `yaml:"app_pass"`

	ReplicationSlot  string `yaml:"replication_slot"`
	Plugin           string `yaml:"plugin"`
	PublicationNames string `yaml:"publication_names"`
}

func (p *PostgresConfig) AppDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s", p.AppUser, p.AppPass, p.Addr, p.DB)
}

func (p *PostgresConfig) ReplicationDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s", p.AppUser, p.AppPass, p.Addr, p.DB)
}

type RedisConfig struct {
	Addr         string `yaml:"addr"`
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	DB           int    `yaml:"db"`
	QMaxAttempts int    `yaml:"query_max_attempts"`
	QDelay       int    `yaml:"query_delay_ms"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	expanded := os.ExpandEnv(string(data))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) Validate() error {
	return c.validate()
}

func (c *Config) validate() error {
	if c.Postgres.Addr == "" {
		return &RequiredError{"postgres.addr"}
	}
	if c.Postgres.ReconnMaxAttempts < 1 {
		return &InvalidValueError{"reconnect_max_attempts must be >= 1", c.Postgres.ReconnMaxAttempts}
	}
	if c.Postgres.ReconnDelayMs < 0 {
		return &InvalidValueError{"reconnect_delay_ms must be >= 0", c.Postgres.ReconnDelayMs}
	}
	if c.Postgres.DB == "" {
		return &RequiredError{"postgres.db"}
	}
	if c.Postgres.ReplicationUser == "" {
		return &RequiredError{"postgres.replication_user"}
	}
	if c.Postgres.AppUser == "" {
		return &RequiredError{"postgres.app_user"}
	}
	if c.Postgres.ReplicationSlot == "" {
		return &RequiredError{"postgres.replication_slot"}
	}
	if c.Postgres.Plugin != "pgoutput" {
		return ErrPostgresPluginUnsupported
	}

	if c.Postgres.PublicationNames == "" {
		return &RequiredError{"postgres.publication_names"}
	}

	if c.Redis.Addr == "" {
		return &RequiredError{"redis.addr"}
	}
	if c.Redis.QMaxAttempts < 1 {
		return &InvalidValueError{"query_max_attempts must be >= 1", c.Redis.QMaxAttempts}
	}
	if c.Redis.QDelay < 0 {
		return &InvalidValueError{"query_delay_ms must be >= 0", c.Redis.QDelay}
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
