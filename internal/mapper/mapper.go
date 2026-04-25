// internal/mapper/mapper.go

// логика маппинга полей postgres и ключей redis
package mapper

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/jackc/pglogrepl"
)

type Mapper struct {
	// отображение полного имени таблицы на правило маппинга
	// - key - "<имя_схемы>.<имя_таблицы>"
	// - value - ссылка на првило маппинга
	qnameRules map[string]*MappingRule

	// отображение RelationID на правила маппинга
	// - key - RelationID
	// - value - ссылка на првило маппинга
	relIDRules map[uint32]*MappingRule
}

type MappingRule struct {
	Table      Table  `yaml:"table"`
	KeyPattern string `yaml:"key_pattern"`
	Compiler   KeyCompiler
}

func New(cfg *MappingConfig) *Mapper {
	qnameRules := make(map[string]*MappingRule)
	for _, rule := range cfg.Rules {
		qnameRules[rule.Table.QualifiedName()] = &rule
	}

	return &Mapper{
		qnameRules,
		make(map[uint32]*MappingRule),
	}
}

// получить <имя_схемы>.<имя_таблицы>
func getQualifiedName(msg *pglogrepl.RelationMessage) string {
	return msg.Namespace + "." + msg.RelationName
}

// закешировать данные об отображении (таблице)
func (m *Mapper) CacheRelation(msg *pglogrepl.RelationMessage) {

	// добавляем данные об отслеживаемом отображении только если есть соответствующее правило
	qName := getQualifiedName(msg)
	if rule, ok := m.qnameRules[qName]; ok {
		m.relIDRules[msg.RelationID] = rule
		for fname, _ := range rule.Compiler.Fields {
			cde := ColumnDataExtracter{}
			for i, col := range msg.Columns {
				if col.Name == fname {
					cde.Idx = i
					cde.DataType = col.DataType
					break
				}
			}
			rule.Compiler.Fields[fname] = cde
		}
	}
}

// получить все измененные ключи, связанные с данным отображением
func (m *Mapper) GetKeys(relID uint32, tuple *pglogrepl.TupleData) ([]string, error) {

	// проверяем отслеживаем ли мы данную таблицу
	rule, ok := m.relIDRules[relID]
	if !ok {
		return nil, nil
	}
	key, err := rule.Compiler.Compile(tuple)
	if err != nil {
		return nil, fmt.Errorf("compile key from tuple: %w", err)
	}
	return []string{key}, nil
}

type ColumnDataExtracter struct {
	Idx      int    // индекс поля в RelationMessage
	DataType uint32 // тип поля
}

func (c ColumnDataExtracter) Extract(tuple *pglogrepl.TupleData) (string, error) {
	return TupleDataToString(tuple.Columns[c.Idx], c.DataType)
}

type KeyCompiler struct {
	Template *template.Template             // шаблон для вставки
	Fields   map[string]ColumnDataExtracter // имя поля таблицы -> данные о поле
}

func (k *KeyCompiler) Compile(tuple *pglogrepl.TupleData) (string, error) {
	dataMap := make(map[string]string)
	for f, e := range k.Fields {
		data, err := e.Extract(tuple)
		if err != nil {
			return "", fmt.Errorf("extract data from tuple: %w", err)
		}
		dataMap[f] = data
	}

	var keyBuilder strings.Builder
	if err := k.Template.Execute(&keyBuilder, dataMap); err != nil {
		return "", fmt.Errorf("execute key template: %w", err)
	}

	return keyBuilder.String(), nil
}

func tmplFromKeyPattern(pattern string) *template.Template {
	// TODO: добавить проверку на равное число { и }
	strTmpl := strings.NewReplacer(
		"{", "{{.",
		"}", "}}",
	).Replace(pattern)

	return template.Must(template.New("").Parse(strTmpl))
}

// привести значение из поля в строку
func TupleDataToString(data *pglogrepl.TupleDataColumn, typ uint32) (string, error) {
	// TODO: сделать функцию TupleDataToString
	return string(data.Data), nil
}

// получить имя ключа, который указан в keyPattern в фигурных скобках
// для user{id} вернет id
func extractColumnName(keyPattern string) (string, error) {
	start := strings.Index(keyPattern, "{")
	end := strings.Index(keyPattern, "}")

	if start == -1 {
		return "", fmt.Errorf("%w in pattern '%s'", ErrMissingOpeningBrace, keyPattern)
	}
	if end == -1 {
		return "", fmt.Errorf("%w in pattern '%s'", ErrMissingClosingBrace, keyPattern)
	}
	if end <= start {
		return "", fmt.Errorf("%w in pattern '%s'", ErrClosingBraceBeforeOpening, keyPattern)
	}

	columnName := keyPattern[start+1 : end]
	if columnName == "" {
		return "", fmt.Errorf("%w in pattern '%s'", ErrEmptyColumnName, keyPattern)
	}

	return columnName, nil
}

// получить имена ключей, которые указаны в keyPattern в фигурных скобках
// - для user{id} вернет []string{"id"}
// - для user{id}:{name} вернет []string{"id", "name"}
func extractColumnNames(keyPattern string) ([]string, error) {
	names := make([]string, 0)
	for {
		start := strings.Index(keyPattern, "{")
		end := strings.Index(keyPattern, "}")

		if start == -1 && end == -1 {
			break
		}

		if start == -1 && end != -1 {
			return nil, fmt.Errorf("%w in pattern '%s'", ErrMissingOpeningBrace, keyPattern)
		}
		if start != -1 && end == -1 {
			return nil, fmt.Errorf("%w in pattern '%s'", ErrMissingClosingBrace, keyPattern)
		}
		if end <= start {
			return nil, fmt.Errorf("%w in pattern '%s'", ErrClosingBraceBeforeOpening, keyPattern)
		}

		name := keyPattern[start+1 : end]
		if name == "" {
			return nil, fmt.Errorf("%w in pattern '%s'", ErrEmptyColumnName, keyPattern)
		}

		names = append(names, name)
		keyPattern = keyPattern[end+1:]
	}

	return names, nil
}
