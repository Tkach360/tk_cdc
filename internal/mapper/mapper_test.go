package mapper

import (
	"errors"
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

func TestMapper_GetKeys(t *testing.T) {
	tests := []struct {
		name           string
		setupRules     map[string]MappingRule
		relationMsg    *pglogrepl.RelationMessage
		tupleData      *pglogrepl.TupleData
		expectedKeys   []string
		expectedExists bool // ожидаем ли ключи (если false, то nil)
	}{
		{
			name: "get key for simple table without schema",
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
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id"},
					{Name: "name"},
				},
			},
			tupleData: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("42")},   // id
					{Data: []byte("John")}, // name
				},
			},
			expectedKeys:   []string{"user:42"},
			expectedExists: true,
		},
		{
			name: "get key for table with custom schema",
			setupRules: map[string]MappingRule{
				"analytics.events": {
					Table:      Table{Schema: "analytics", Name: "events"},
					KeyPattern: "event:{event_id}",
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   67890,
				RelationName: "events",
				Namespace:    "analytics",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "event_id"},
					{Name: "user_id"},
				},
			},
			tupleData: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("12345")}, // event_id
					{Data: []byte("678")},   // user_id
				},
			},
			expectedKeys:   []string{"event:12345"},
			expectedExists: true,
		},
		{
			name: "get key with text column",
			setupRules: map[string]MappingRule{
				"public.products": {
					Table:      Table{Schema: "public", Name: "products"},
					KeyPattern: "product:{sku}",
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   11111,
				RelationName: "products",
				Namespace:    "public",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "sku"},
					{Name: "price"},
				},
			},
			tupleData: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("ABC-123")},
					{Data: []byte("99.99")},
				},
			},
			expectedKeys:   []string{"product:ABC-123"},
			expectedExists: true,
		},
		{
			name: "return nil when relation not tracked",
			setupRules: map[string]MappingRule{
				"public.orders": {
					Table:      Table{Schema: "public", Name: "orders"},
					KeyPattern: "order:{id}",
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   99999,
				RelationName: "users",
				Namespace:    "public",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id"},
				},
			},
			tupleData: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("100")},
				},
			},
			expectedKeys:   nil,
			expectedExists: false,
		},
		{
			name: "handle multiple rules with different tables",
			setupRules: map[string]MappingRule{
				"public.users": {
					Table:      Table{Schema: "public", Name: "users"},
					KeyPattern: "user:{id}",
				},
				"public.orders": {
					Table:      Table{Schema: "public", Name: "orders"},
					KeyPattern: "order:{order_id}",
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   77777,
				RelationName: "orders",
				Namespace:    "public",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "order_id"},
					{Name: "user_id"},
				},
			},
			tupleData: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("500")},
					{Data: []byte("42")},
				},
			},
			expectedKeys:   []string{"order:500"},
			expectedExists: true,
		},
		{
			name: "handle key pattern without curly braces",
			setupRules: map[string]MappingRule{
				"public.simple": {
					Table:      Table{Schema: "public", Name: "simple"},
					KeyPattern: "static_key",
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   44444,
				RelationName: "simple",
				Namespace:    "public",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id"},
				},
			},
			tupleData: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("123")},
				},
			},
			expectedKeys:   []string{"static_key"},
			expectedExists: true,
		},
		{
			name: "handle null column value (currently returns empty string)",
			setupRules: map[string]MappingRule{
				"public.nullable": {
					Table:      Table{Schema: "public", Name: "nullable"},
					KeyPattern: "key:{value}",
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   33333,
				RelationName: "nullable",
				Namespace:    "public",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "value"},
				},
			},
			tupleData: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: nil},
				},
			},
			expectedKeys:   []string{"key:"},
			expectedExists: true,
		},
		// TODO: при реализации ключей с множественными полями добавить тест
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Создаем маппер и кешируем relation
			mapper := &Mapper{
				rules:   tt.setupRules,
				relData: make(map[uint32]*pglogrepl.RelationMessage),
			}

			// Кешируем relation
			mapper.CacheRelation(tt.relationMsg)

			// Вызываем GetKeys
			keys := mapper.GetKeys(tt.relationMsg.RelationID, tt.tupleData)

			if !tt.expectedExists {
				assert.Nil(t, keys, "Expected nil keys for untracked relation")
			} else {
				assert.NotNil(t, keys, "Expected non-nil keys")
				assert.Equal(t, tt.expectedKeys, keys, "Keys don't match expected")
			}
		})
	}
}

func TestMapper_GetKeys_DifferentDataTypes(t *testing.T) {
	testCases := []struct {
		name     string
		dataType uint32
		data     []byte
		expected string
	}{
		{
			name:     "int4",
			data:     []byte("12345"),
			expected: "key:12345",
		},
		{
			name:     "int8",
			data:     []byte("9223372036854775807"),
			expected: "key:9223372036854775807",
		},
		{
			name:     "text",
			data:     []byte("some text value"),
			expected: "key:some text value",
		},
		{
			name:     "bool true",
			data:     []byte("t"),
			expected: "key:t",
		},
		{
			name:     "float8",
			data:     []byte("123.456"),
			expected: "key:123.456",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mapper := &Mapper{
				rules: map[string]MappingRule{
					"public.test": {
						Table:      Table{Schema: "public", Name: "test"},
						KeyPattern: "key:{value}",
					},
				},
				relData: make(map[uint32]*pglogrepl.RelationMessage),
			}

			relationMsg := &pglogrepl.RelationMessage{
				RelationID:   999,
				RelationName: "test",
				Namespace:    "public",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "value", DataType: tc.dataType},
				},
			}
			mapper.CacheRelation(relationMsg)

			tupleData := &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: tc.data},
				},
			}

			keys := mapper.GetKeys(999, tupleData)
			assert.Equal(t, []string{tc.expected}, keys)
		})
	}
}

func TestExtractColumnName(t *testing.T) {
	tests := []struct {
		name        string
		keyPattern  string
		expected    string
		expectedErr error
	}{
		{
			name:        "valid pattern with simple placeholder",
			keyPattern:  "user:{id}",
			expected:    "id",
			expectedErr: nil,
		},
		{
			name:        "valid pattern with underscore",
			keyPattern:  "user:{user_id}",
			expected:    "user_id",
			expectedErr: nil,
		},
		{
			name:        "valid pattern with text before and after braces",
			keyPattern:  "prefix_{code}_suffix",
			expected:    "code",
			expectedErr: nil,
		},
		{
			name:        "valid pattern with only braces",
			keyPattern:  "{id}",
			expected:    "id",
			expectedErr: nil,
		},
		{
			name:        "valid pattern with special chars in placeholder",
			keyPattern:  "user:{user-id}",
			expected:    "user-id",
			expectedErr: nil,
		},
		{
			name:        "multiple braces - returns first placeholder",
			keyPattern:  "user:{id}:{name}",
			expected:    "id",
			expectedErr: nil,
		},
		{
			name:        "missing opening brace",
			keyPattern:  "user:id}",
			expected:    "",
			expectedErr: ErrMissingOpeningBrace,
		},
		{
			name:        "missing closing brace",
			keyPattern:  "user:{id",
			expected:    "",
			expectedErr: ErrMissingClosingBrace,
		},
		{
			name:        "empty column name",
			keyPattern:  "user:{}",
			expected:    "",
			expectedErr: ErrEmptyColumnName,
		},
		{
			name:        "closing brace before opening brace",
			keyPattern:  "user:}id{",
			expected:    "",
			expectedErr: ErrClosingBraceBeforeOpening,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := extractColumnName(tt.keyPattern)

			if tt.expectedErr != nil {
				assert.Error(t, err)
				assert.True(t, errors.Is(err, tt.expectedErr),
					"Expected error %v, got %v", tt.expectedErr, err)
				assert.Empty(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
