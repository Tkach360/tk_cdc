// internal/mapper/mapper.go

// логика маппинга полей postgres и ключей redis
package mapper

import (
	"strings"

	"github.com/Tkach360/tk_cdc/internal/config"
	"github.com/jackc/pglogrepl"
)

type Mapper struct {
	// правила маппинга
	// - key - имя отображения (таблицы) в виде схема.имя
	// - value - собственно правило
	rules map[string]config.MappingRule

	// хеш-таблица данных об отношениях
	// - key - RelationID
	// - value - *pglogrepl.RelationMessage
	relData map[uint32]*pglogrepl.RelationMessage
}

func New(rules []config.MappingRule) *Mapper {

	mrules := make(map[string]config.MappingRule)
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
func normalizeRelationName(name string) string {
	if !strings.Contains(name, ".") {
		// TODO: не забыть добавить в документацию, что нужно указывать схему иначе схема public
		// TODO: может сделать указание схемы по-умолчанию в конфиге?
		name = "public." + name
	}
	return name
}

// закешировать данные об отображении (таблице)
func (m *Mapper) CacheRelation(msg *pglogrepl.RelationMessage) {

	// добавляем данные об отслеживаемом отображении только если есть соответствующее правило
	relName := normalizeRelationName(msg.RelationName)
	if _, ok := m.rules[relName]; ok {
		m.relData[msg.RelationID] = msg
	}
}

// получить все измененные ключи, связанные с данным отображением
func (s *Mapper) GetKeys(relID uint32, tuple *pglogrepl.TupleData) []string {
	// TODO: сделать метод *Mapper.GetKeys
	return []string{}
}
