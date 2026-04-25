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

		names, err := extractColumnNames(ruleRaw.KeyPattern)
		if err != nil {
			return fmt.Errorf("extract column names: %w", err)
		}
		if len(names) == 0 {
			return fmt.Errorf("the number of fields in key_pattern cannot be equal to 0")
		}

		fields := make(map[string]ColumnDataExtracter, len(names))
		for _, name := range names {
			fields[name] = ColumnDataExtracter{} // будет добавляться при кешировании данных таблицы
		}

		mc.Rules[i] = MappingRule{
			Table:      table,
			KeyPattern: ruleRaw.KeyPattern,
			Compiler: KeyCompiler{
				Template: tmplFromKeyPattern(ruleRaw.KeyPattern),
				Fields:   fields,
			},
		}
	}

	return nil
}
