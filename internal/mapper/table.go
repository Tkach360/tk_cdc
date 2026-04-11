package mapper

import (
	"fmt"
)

// структура с данными таблицы
type Table struct {
	Schema string
	Name   string
}

// получить полное имя таблицы в виде <схема>.<таблица>
func (t Table) QualifiedName() string {
	return t.Schema + "." + t.Name
}

func (t *Table) UnmarshalYAML(unmarshal func(any) error) error {
	var raw string
	if err := unmarshal(&raw); err != nil {
		return err
	}
	schema, name, err := parseTableString(raw)
	if err != nil {
		return err
	}
	t.Schema = schema
	t.Name = name
	return nil
}

// разбить строку формата "schema.table" или "table"
// с учётом возможных двойных кавычек вокруг частей
func parseTableString(s string) (schema, table string, err error) {
	inQuotes := false
	separatorIdx := -1

	for i, ch := range s {
		if ch == '"' {
			inQuotes = !inQuotes
		} else if ch == '.' && !inQuotes {
			separatorIdx = i
			break
		}
	}

	if separatorIdx == -1 {
		// если нет разделителей, значит вся строка имя таблицы, а схема public
		// TODO: тут нужно учитывать какая схема по умолчанию
		return "", unquote(s), nil
	}

	schemaPart := s[:separatorIdx]
	tablePart := s[separatorIdx+1:]

	schema = unquote(schemaPart)
	table = unquote(tablePart)

	// if schema == "" {
	// 	return "", "", fmt.Errorf("empty schema name in %q", s)
	// }
	if table == "" {
		return "", "", fmt.Errorf("empty table name in %q", s)
	}
	return schema, table, nil
}

// удалить обрамляющие двойные кавычки если они есть
func unquote(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}
