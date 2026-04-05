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
		key := rule.Table

		// нормализуем, чтобы обязательно была схема, если схема не указана, то схема public
		// TODO: не забыть добавить в документацию, что нужно указывать схему иначе схема public
		if !strings.Contains(rule.Table, ".") {
			key = "public." + rule.Table
		}
		mrules[key] = rule
	}

	return &Mapper{
		mrules,
		make(map[uint32]*pglogrepl.RelationMessage),
	}
}

// закешировать данные об отображении (таблице)
func (m *Mapper) CacheRelation(msg *pglogrepl.RelationMessage) {
	m.relData[msg.RelationID] = msg
}

// получить все измененные ключи, связанные с данным отображением
func (s *Mapper) GetKeys(relID uint32, tuple *pglogrepl.TupleData) []string {
	// TODO: сделать метод *Mapper.GetKeys
	return []string{}
}
