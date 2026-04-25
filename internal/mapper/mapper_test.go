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
					Compiler: KeyCompiler{
						Fields: map[string]ColumnDataExtracter{
							"id": {},
						},
					},
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   12345,
				RelationName: "users",
				Namespace:    "public",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: 23},
					{Name: "name", DataType: 25},
				},
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
					Compiler: KeyCompiler{
						Fields: map[string]ColumnDataExtracter{
							"id": {},
						},
					},
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   12345,
				RelationName: "users",
				Namespace:    "public",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: 23},
					{Name: "name", DataType: 25},
				},
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
					Compiler: KeyCompiler{
						Fields: map[string]ColumnDataExtracter{
							"id": {},
						},
					},
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   67890,
				RelationName: "events",
				Namespace:    "analytics",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: 23},
					{Name: "event_name", DataType: 25},
				},
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
					Compiler: KeyCompiler{
						Fields: map[string]ColumnDataExtracter{
							"number": {},
						},
					},
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   54321,
				RelationName: "users",
				Namespace:    "public",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: 23},
				},
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
					Compiler: KeyCompiler{
						Fields: map[string]ColumnDataExtracter{
							"id": {},
						},
					},
				},
				"public.orders": {
					Table:      Table{Schema: "public", Name: "orders"},
					KeyPattern: "order:{number}",
					Compiler: KeyCompiler{
						Fields: map[string]ColumnDataExtracter{
							"number": {},
						},
					},
				},
				"analytics.events": {
					Table:      Table{Schema: "analytics", Name: "events"},
					KeyPattern: "event:{id}",
					Compiler: KeyCompiler{
						Fields: map[string]ColumnDataExtracter{
							"id": {},
						},
					},
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   11111,
				RelationName: "events",
				Namespace:    "analytics",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: 23},
					{Name: "user_id", DataType: 23},
				},
			},
			expectedCached: true,
			expectedRelID:  11111,
		},
		{
			name: "handle relation with quoted name",
			setupRules: map[string]MappingRule{
				"my-schema.users": {
					Table:      Table{Schema: "my-schema", Name: "users"},
					KeyPattern: "user:{id}",
					Compiler: KeyCompiler{
						Fields: map[string]ColumnDataExtracter{
							"id": {},
						},
					},
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   99999,
				RelationName: "users",
				Namespace:    "my-schema",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: 23},
				},
			},
			expectedCached: true,
			expectedRelID:  99999,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rulesCopy := make(map[string]*MappingRule)
			for k, v := range tt.setupRules {
				ruleCopy := v
				fieldsCopy := make(map[string]ColumnDataExtracter)
				for fk, fv := range v.Compiler.Fields {
					fieldsCopy[fk] = fv
				}
				ruleCopy.Compiler.Fields = fieldsCopy
				rulesCopy[k] = &ruleCopy
			}

			cfg := &MappingConfig{Rules: func() []MappingRule {
				rules := make([]MappingRule, 0, len(tt.setupRules))
				for _, rule := range tt.setupRules {
					rules = append(rules, rule)
				}
				return rules
			}()}

			mapper := New(cfg)

			_, exists := mapper.qnameRules[getQualifiedName(tt.relationMsg)]
			if tt.expectedCached {
				assert.True(t, exists, "Rule should exist in qnameRules before caching")
			}

			assert.Empty(t, mapper.relIDRules, "relIDRules should be empty before caching")

			mapper.CacheRelation(tt.relationMsg)

			if tt.expectedCached {
				cachedRule, existsInRelID := mapper.relIDRules[tt.relationMsg.RelationID]
				assert.True(t, existsInRelID, "Rule should be cached in relIDRules")
				assert.NotNil(t, cachedRule, "Cached rule should not be nil")

				for fieldName, extracter := range cachedRule.Compiler.Fields {
					assert.NotEqual(t, -1, extracter.Idx, "Field index should be set for %s", fieldName)
					assert.NotEqual(t, uint32(0), extracter.DataType, "Field data type should be set for %s", fieldName)

					found := false
					for i, col := range tt.relationMsg.Columns {
						if col.Name == fieldName {
							assert.Equal(t, i, extracter.Idx, "Field index should match column position")
							assert.Equal(t, col.DataType, extracter.DataType, "Field data type should match column data type")
							found = true
							break
						}
					}
					assert.True(t, found, "Field %s should exist in relation columns", fieldName)
				}

				originalRule := rulesCopy[getQualifiedName(tt.relationMsg)]
				assert.NotNil(t, originalRule, "Original rule should exist")
			} else {
				_, existsInRelID := mapper.relIDRules[tt.relationMsg.RelationID]
				assert.False(t, existsInRelID, "Rule should not be cached in relIDRules when no matching rule exists")
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
		expectedExists bool
	}{
		{
			name: "get key for simple table without schema",
			setupRules: map[string]MappingRule{
				"public.users": {
					Table:      Table{Schema: "public", Name: "users"},
					KeyPattern: "user:{id}",
					Compiler: KeyCompiler{
						Template: tmplFromKeyPattern("user:{id}"),
						Fields: map[string]ColumnDataExtracter{
							"id": {},
						},
					},
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   12345,
				RelationName: "users",
				Namespace:    "public",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: 23},
					{Name: "name", DataType: 25},
				},
			},
			tupleData: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("42")},
					{Data: []byte("John")},
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
					Compiler: KeyCompiler{
						Template: tmplFromKeyPattern("event:{event_id}"),
						Fields: map[string]ColumnDataExtracter{
							"event_id": {},
						},
					},
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   67890,
				RelationName: "events",
				Namespace:    "analytics",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "event_id", DataType: 23},
					{Name: "user_id", DataType: 23},
				},
			},
			tupleData: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("12345")},
					{Data: []byte("678")},
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
					Compiler: KeyCompiler{
						Template: tmplFromKeyPattern("product:{sku}"),
						Fields: map[string]ColumnDataExtracter{
							"sku": {},
						},
					},
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   11111,
				RelationName: "products",
				Namespace:    "public",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "sku", DataType: 25},
					{Name: "price", DataType: 701},
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
					Compiler: KeyCompiler{
						Template: tmplFromKeyPattern("order:{id}"),
						Fields: map[string]ColumnDataExtracter{
							"id": {},
						},
					},
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   99999,
				RelationName: "users",
				Namespace:    "public",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: 23},
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
					Compiler: KeyCompiler{
						Template: tmplFromKeyPattern("user:{id}"),
						Fields: map[string]ColumnDataExtracter{
							"id": {},
						},
					},
				},
				"public.orders": {
					Table:      Table{Schema: "public", Name: "orders"},
					KeyPattern: "order:{order_id}",
					Compiler: KeyCompiler{
						Template: tmplFromKeyPattern("order:{order_id}"),
						Fields: map[string]ColumnDataExtracter{
							"order_id": {},
						},
					},
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   77777,
				RelationName: "orders",
				Namespace:    "public",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "order_id", DataType: 23},
					{Name: "user_id", DataType: 23},
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
					Compiler: KeyCompiler{
						Template: tmplFromKeyPattern("static_key"),
						Fields:   map[string]ColumnDataExtracter{},
					},
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   44444,
				RelationName: "simple",
				Namespace:    "public",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: 23},
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
			name: "handle null column value",
			setupRules: map[string]MappingRule{
				"public.nullable": {
					Table:      Table{Schema: "public", Name: "nullable"},
					KeyPattern: "key:{value}",
					Compiler: KeyCompiler{
						Template: tmplFromKeyPattern("key:{value}"),
						Fields: map[string]ColumnDataExtracter{
							"value": {},
						},
					},
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   33333,
				RelationName: "nullable",
				Namespace:    "public",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "value", DataType: 25},
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
		{
			name: "get key with multiple fields in pattern",
			setupRules: map[string]MappingRule{
				"public.composite": {
					Table:      Table{Schema: "public", Name: "composite"},
					KeyPattern: "composite:{id}:{tenant}",
					Compiler: KeyCompiler{
						Template: tmplFromKeyPattern("composite:{id}:{tenant}"),
						Fields: map[string]ColumnDataExtracter{
							"id":     {},
							"tenant": {},
						},
					},
				},
			},
			relationMsg: &pglogrepl.RelationMessage{
				RelationID:   55555,
				RelationName: "composite",
				Namespace:    "public",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: 23},
					{Name: "tenant", DataType: 25},
					{Name: "data", DataType: 25},
				},
			},
			tupleData: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("42")},
					{Data: []byte("tenant-abc")},
					{Data: []byte("some data")},
				},
			},
			expectedKeys:   []string{"composite:42:tenant-abc"},
			expectedExists: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &MappingConfig{
				DefaultSchema: "public",
				Rules:         make([]MappingRule, 0, len(tt.setupRules)),
			}

			for _, rule := range tt.setupRules {
				cfg.Rules = append(cfg.Rules, rule)
			}

			mapper := New(cfg)

			for _, rule := range cfg.Rules {
				qname := rule.Table.QualifiedName()
				cachedRule, exists := mapper.qnameRules[qname]
				assert.True(t, exists, "Rule should exist in qnameRules")
				assert.NotNil(t, cachedRule.Compiler.Fields, "Compiler.Fields should not be nil")

				if rule.KeyPattern != "static_key" {
					assert.NotEmpty(t, cachedRule.Compiler.Fields, "Compiler.Fields should not be empty for patterns with placeholders")
				}
			}

			mapper.CacheRelation(tt.relationMsg)

			keys, _ := mapper.GetKeys(tt.relationMsg.RelationID, tt.tupleData)

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
			cfg := &MappingConfig{
				DefaultSchema: "public",
				Rules: []MappingRule{
					{
						Table:      Table{Schema: "public", Name: "test"},
						KeyPattern: "key:{value}",
						Compiler: KeyCompiler{
							Template: tmplFromKeyPattern("key:{value}"),
							Fields: map[string]ColumnDataExtracter{
								"value": {},
							},
						},
					},
				},
			}

			mapper := New(cfg)

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

			keys, _ := mapper.GetKeys(999, tupleData)
			assert.Equal(t, []string{tc.expected}, keys)
		})
	}
}

func TestExtractColumnNames(t *testing.T) {
	tests := []struct {
		name        string
		keyPattern  string
		expected    []string
		expectedErr error
	}{
		{
			name:        "valid pattern with simple placeholder",
			keyPattern:  "user:{id}",
			expected:    []string{"id"},
			expectedErr: nil,
		},
		{
			name:        "valid pattern with underscore",
			keyPattern:  "user:{user_id}",
			expected:    []string{"user_id"},
			expectedErr: nil,
		},
		{
			name:        "valid pattern with text before and after braces",
			keyPattern:  "prefix_{code}_suffix",
			expected:    []string{"code"},
			expectedErr: nil,
		},
		{
			name:        "valid pattern with only braces",
			keyPattern:  "{id}",
			expected:    []string{"id"},
			expectedErr: nil,
		},
		{
			name:        "valid pattern with special chars in placeholder",
			keyPattern:  "user:{user-id}",
			expected:    []string{"user-id"},
			expectedErr: nil,
		},
		{
			name:        "multiple braces - returns all placeholders",
			keyPattern:  "user:{id}:{name}",
			expected:    []string{"id", "name"},
			expectedErr: nil,
		},
		{
			name:        "multiple braces with text between",
			keyPattern:  "{first}_{second}_{third}",
			expected:    []string{"first", "second", "third"},
			expectedErr: nil,
		},
		{
			name:        "missing opening brace",
			keyPattern:  "user:id}",
			expected:    nil,
			expectedErr: ErrMissingOpeningBrace,
		},
		{
			name:        "missing closing brace",
			keyPattern:  "user:{id",
			expected:    nil,
			expectedErr: ErrMissingClosingBrace,
		},
		{
			name:        "empty column name",
			keyPattern:  "user:{}",
			expected:    nil,
			expectedErr: ErrEmptyColumnName,
		},
		{
			name:        "closing brace before opening brace",
			keyPattern:  "user:}id{",
			expected:    nil,
			expectedErr: ErrClosingBraceBeforeOpening,
		},
		{
			name:        "empty placeholder in multiple braces",
			keyPattern:  "user:{id}:{}:{name}",
			expected:    nil,
			expectedErr: ErrEmptyColumnName,
		},
		{
			name:        "nested braces - invalid",
			keyPattern:  "user:{id:{name}}",
			expected:    nil,
			expectedErr: ErrMissingOpeningBrace,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := extractColumnNames(tt.keyPattern)

			if tt.expectedErr != nil {
				assert.Error(t, err)
				assert.True(t, errors.Is(err, tt.expectedErr),
					"Expected error \"%v\", got \"%v\"", tt.expectedErr, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
