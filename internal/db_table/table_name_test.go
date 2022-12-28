package db_table

import (
	"testing"
	"time"
)

func TestGenerateTableName(t *testing.T) {
	cases := []struct {
		name    string
		pattern string
		tags    map[string]any
		expect  string
	}{
		{
			name:    "1",
			pattern: "",
			tags:    map[string]any{"a": "a", "b": "b"},
			expect:  "t_187ef4436122d1cc2f40dc2b92f0eba0",
		},
		{
			name:    "2",
			pattern: "d_{code}_{name}",
			tags:    map[string]any{"code": "100", "name": "aaa"},
			expect:  "d_100_aaa",
		},
		{
			name:    "3",
			pattern: "d_{code}_{name}",
			tags:    map[string]any{"code": "100"},
			expect:  "t_f899139df5e1059396431415e770c6dd",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tableName, err := GenerateTableName(c.pattern, c.tags)
			if err != nil {
				t.Fatal(err)
			}
			if tableName != c.expect {
				t.Fatalf("generate table name fail, expect-[%s], but got-[%s]", c.expect, tableName)
			}
		})
	}
}

func TestTimeFormat(t *testing.T) {
	t.Log(time.Parse("20060102150405.000", "20221123094625.100"))
	t.Log(time.Parse("20060102150405.000", "20221123094625100"))
}
