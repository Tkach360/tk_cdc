package mapper

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
)

func TestMapper_CacheRelation(t *testing.T) {
	tests := []struct {
		name           string
		setupRules     map[string]MappingRule
		relationMsg    *pglogrepl.RelationMessage
		expectedCached bool
		expectedRelID  uint32
	}{
		{
			name: "cache relation when rule exists (without schema)",
			setupRules: map[string]MappingRule{
				"public.users": {
					Table:      Table{Schema: "public", Name: "users"},
					KeyPattern: "user:{id}",
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   12345,
				RelationName: "users",
				Namespace:    "public",
				Columns:      []*pglogrepl.RelationMessageColumn{},
			},
			expectedCached: true,
			expectedRelID:  12345,
		},
		{
			name: "cache relation when rule exists (with schema in rule)",
			setupRules: map[string]MappingRule{
				"public.users": {
					Table:      Table{Schema: "public", Name: "users"},
					KeyPattern: "user:{id}",
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   12345,
				RelationName: "users",
				Namespace:    "public",
				Columns:      []*pglogrepl.RelationMessageColumn{},
			},
			expectedCached: true,
			expectedRelID:  12345,
		},
		{
			name: "cache relation with custom schema",
			setupRules: map[string]MappingRule{
				"analytics.events": {
					Table:      Table{Schema: "analytics", Name: "events"},
					KeyPattern: "event:{id}",
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   67890,
				RelationName: "events",
				Namespace:    "analytics",
				Columns:      []*pglogrepl.RelationMessageColumn{},
			},
			expectedCached: true,
			expectedRelID:  67890,
		},
		{
			name: "do not cache when rule does not exist",
			setupRules: map[string]MappingRule{
				"public.orders": {
					Table:      Table{Schema: "public", Name: "orders"},
					KeyPattern: "order:{number}",
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   54321,
				RelationName: "users",
				Namespace:    "public",
				Columns:      []*pglogrepl.RelationMessageColumn{},
			},
			expectedCached: false,
			expectedRelID:  54321,
		},
		{
			name: "cache multiple relations",
			setupRules: map[string]MappingRule{
				"public.users": {
					Table:      Table{Schema: "public", Name: "users"},
					KeyPattern: "user:{id}",
				},
				"public.orders": {
					Table:      Table{Schema: "public", Name: "orders"},
					KeyPattern: "order:{number}",
				},
				"analytics.events": {
					Table:      Table{Schema: "analytics", Name: "events"},
					KeyPattern: "event:{id}",
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   11111,
				RelationName: "events",
				Namespace:    "analytics",
				Columns:      []*pglogrepl.RelationMessageColumn{},
			},
			expectedCached: true,
			expectedRelID:  11111,
		},
		{
			name: "handle relation with quoted name",
			setupRules: map[string]MappingRule{
				"my-schema.users": { // нормализованный ключ без кавычек
					Table:      Table{Schema: "my-schema", Name: "users"},
					KeyPattern: "user:{id}",
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   99999,
				RelationName: "users",
				Namespace:    "my-schema",
				Columns:      []*pglogrepl.RelationMessageColumn{},
			},
			expectedCached: true,
			expectedRelID:  99999,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper := &Mapper{
				rules:   tt.setupRules,
				relData: make(map[uint32]*pglogrepl.RelationMessage),
			}

			mapper.CacheRelation(tt.relationMsg)

			cachedMsg, exists := mapper.relData[tt.relationMsg.RelationID]

			if tt.expectedCached {
				assert.True(t, exists, "Relation should be cached")
				assert.NotNil(t, cachedMsg, "Cached message should not be nil")
				assert.Equal(t, tt.relationMsg, cachedMsg, "Cached message should match original")
				assert.Equal(t, tt.expectedRelID, cachedMsg.RelationID, "Relation ID should match")
			} else {
				assert.False(t, exists, "Relation should not be cached when no matching rule exists")
			}
		})
	}
}
