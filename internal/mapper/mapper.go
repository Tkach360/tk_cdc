// internal/mapper/mapper.go

// логика маппинга полей postgres и ключей redis
package mapper

import (
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
)

type Mapper struct {
	// правила маппинга
	// - key - имя отображения (таблицы) в виде схема.имя
	// - value - собственно правило
	rules map[string]MappingRule

	// хеш-таблица данных об отношениях
	// - key - RelationID
	// - value - *pglogrepl.RelationMessage
	relData map[uint32]*pglogrepl.RelationMessage
}

type MappingRule struct {
	Table      string `yaml:"table"`
	KeyPattern string `yaml:"key_pattern"`
}

func New(rules []MappingRule) *Mapper {

	mrules := make(map[string]MappingRule)
	for _, rule := range rules {
		key := normalizeRelationName(rule.Table)
		mrules[key] = rule
	}

	return &Mapper{
		mrules,
		make(map[uint32]*pglogrepl.RelationMessage),
	}
}

// нормализация имени отношения - добавление имени схемы перед именем таблицы если его нет
// вернет имя_схемы.имя_таблицы, если схемы нет, то схема public
// убирает двойные и одинарные кавычки
func normalizeRelationName(name string) string {
	name = strings.ReplaceAll(name, "\"", "")
	name = strings.ReplaceAll(name, "'", "")

	if !strings.Contains(name, ".") {
		// TODO: не забыть добавить в документацию, что нужно указывать схему иначе схема public
		// TODO: может сделать указание схемы по-умолчанию в конфиге?
		name = "public." + name
	}
	return name
}

// получить <имя_схемы>.<имя_таблицы>
func getQualifiedName(msg *pglogrepl.RelationMessage) string {
	return msg.Namespace + "." + msg.RelationName
}

// закешировать данные об отображении (таблице)
func (m *Mapper) CacheRelation(msg *pglogrepl.RelationMessage) {

	// добавляем данные об отслеживаемом отображении только если есть соответствующее правило
	relName := getQualifiedName(msg)
	if _, ok := m.rules[relName]; ok {
		m.relData[msg.RelationID] = msg
	}
}

// получить все измененные ключи, связанные с данным отображением
func (m *Mapper) GetKeys(relID uint32, tuple *pglogrepl.TupleData) []string {

	// проверяем отслеживаем ли мы данную таблицу
	relMsg, ok := m.relData[relID]
	if !ok {
		return nil
	}

	relName := normalizeRelationName(relMsg.RelationName)
	rule := m.rules[relName]

	// TODO: явно нужно парсить KeyPattern в какую-то структуру и тут уже использовать всё готовое
	colName, _ := extractColumnName(rule.KeyPattern)

	// TODO: индекс поля и тип следует сразу где-то считать, а не вычислять каждый раз
	var colIdx int
	var colType uint32
	for i, col := range relMsg.Columns {
		if col.Name == colName {
			colIdx = i
			colType = col.DataType
		}
	}

	// TODO: нужно обрабатывать ошибку, если например в поле NULL
	inkey, _ := TupleDataToString(tuple.Columns[colIdx], colType)

	// TODO: это тоже явно нужно переделать
	key := strings.ReplaceAll(rule.KeyPattern, "{"+colName+"}", inkey)

	// TODO: пока что возвращаю 1 ключ, в дальнейшем нужно будет реализовать составной ключ
	return []string{key}
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
		return "", fmt.Errorf("missing opening brace '{'")
	}
	if end == -1 {
		return "", fmt.Errorf("missing closing brace '}'")
	}
	if end <= start {
		return "", fmt.Errorf("closing brace appears before opening brace")
	}

	columnName := keyPattern[start+1 : end]
	if columnName == "" {
		return "", fmt.Errorf("empty column name between braces")
	}

	return columnName, nil
}
