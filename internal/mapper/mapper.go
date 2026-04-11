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
	Table      Table  `yaml:"table"`
	KeyPattern string `yaml:"key_pattern"`
}

func New(rules []MappingRule) *Mapper {

	mrules := make(map[string]MappingRule)
	for _, rule := range rules {
		mrules[rule.Table.QualifiedName()] = rule
	}

	return &Mapper{
		mrules,
		make(map[uint32]*pglogrepl.RelationMessage),
	}
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

	relName := relMsg.Namespace + "." + relMsg.RelationName
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
