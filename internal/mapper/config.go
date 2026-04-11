package mapper

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

type MappingConfig struct {
	DefaultSchema string        `yaml:"default_schema"`
	Rules         []MappingRule `yaml:"rules"`
}

// временная структура для парсинга сырых правил
type MappingRuleRaw struct {
	Table      string `yaml:"table"`
	KeyPattern string `yaml:"key_pattern"`
}

var DEFAULT_SCHEMA = "public"

// кастомный парсер, если в имени таблицы не указана схема, то подставляется параметр default_schema
// если default_schema не указана, то public
func (mc *MappingConfig) UnmarshalYAML(value *yaml.Node) error {
	var raw struct {
		DefaultSchema string `yaml:"default_schema"`
		Rules         []struct {
			Table      yaml.Node `yaml:"table"`
			KeyPattern string    `yaml:"key_pattern"`
		} `yaml:"rules"`
	}

	if err := value.Decode(&raw); err != nil {
		return err
	}

	mc.DefaultSchema = raw.DefaultSchema
	if mc.DefaultSchema == "" {
		mc.DefaultSchema = DEFAULT_SCHEMA
	}

	mc.Rules = make([]MappingRule, len(raw.Rules))
	for i, ruleRaw := range raw.Rules {
		var table Table
		if err := ruleRaw.Table.Decode(&table); err != nil {
			return fmt.Errorf("rules[%d]: %w", i, err)
		}

		if table.Schema == "" {
			table.Schema = mc.DefaultSchema
		}

		mc.Rules[i] = MappingRule{
			Table:      table,
			KeyPattern: ruleRaw.KeyPattern,
		}
	}

	return nil
}
