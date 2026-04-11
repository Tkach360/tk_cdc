package config

import (
	"errors"
	"fmt"
)

var (
	ErrPostgresDSNRequired              = errors.New("postgres.dsn is required")
	ErrPostgresReplicationSlotRequired  = errors.New("postgres.replication_slot is required")
	ErrPostgresPluginUnsupported        = errors.New("only 'pgoutput' plugin is supported")
	ErrRedisAddrRequired                = errors.New("redis.addr is required")
	ErrMappingTableRequired             = errors.New("mapping: table is required")
	ErrMappingKeyPatternRequired        = errors.New("mapping: key_pattern is required")
	ErrMappingKeyPatternNoPlaceholder   = errors.New("mapping: key_pattern must contain {field} placeholder")
	ErrPostgresPublicationNamesRequired = errors.New("postgres.publication_names is required when using pgoutput plugin")
)

type MappingError struct {
	Index int
	Err   error
}

func (e *MappingError) Error() string {
	return fmt.Sprintf("mapping[%d]: %v", e.Index, e.Err)
}

func (e *MappingError) Unwrap() error {
	return e.Err
}
