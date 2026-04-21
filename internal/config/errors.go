package config

import (
	"errors"
	"fmt"
)

var (
	ErrPostgresPluginUnsupported      = errors.New("only 'pgoutput' plugin is supported")
	ErrMappingTableRequired           = errors.New("mapping: table is required")
	ErrMappingKeyPatternRequired      = errors.New("mapping: key_pattern is required")
	ErrMappingKeyPatternNoPlaceholder = errors.New("mapping: key_pattern must contain {field} placeholder")
)

type RequiredError struct {
	Name string
}

func (e *RequiredError) Error() string {
	return fmt.Sprintf("%s is required", e.Name)
}

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

type InvalidValueError struct {
	condition string
	value     any
}

func (e *InvalidValueError) Error() string {
	return fmt.Sprintf("%s, have: %v", e.condition, e.value)
}
