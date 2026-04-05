// internal/mapper/mapper.go

// логика маппинга полей postgres и ключей redis
package mapper

import (
	"github.com/Tkach360/tk_cdc/internal/config"
	"github.com/jackc/pglogrepl"
)

type Mapper struct {
}

func New(mrules []config.MappingRule) *Mapper {
	// TODO: сделать mapper.New
	return nil
}

// закешировать данные об отображении (таблице)
func (m *Mapper) CacheRelation(msg *pglogrepl.RelationMessage) {
	// TODO: сделать метод *Mapper.CacheRelation
}
