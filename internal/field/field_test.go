package field

import (
	"testing"
	"time"
)

func TestExtract(t *testing.T) {
	cases := []struct {
		name       string
		expression string
		data       map[string]any
		expect     any
	}{
		{
			name:       "1",
			expression: "left_pad(name, \"0\", 10)",
			data:       map[string]any{"name": "test", "code": 123123},
			expect:     "000000test",
		},
		{
			name:       "2",
			expression: "date_parse(date + left_pad(time, \"0\", 9), \"YYYYMMDDHHmmssSSS\")",
			data:       map[string]any{"date": "20221123", "time": "94625100"},
			expect:     time.Date(2022, 11, 23, 9, 46, 25, 100000000, time.UTC),
		},
		{
			name:       "3",
			expression: "a + b",
			data:       map[string]any{"a": 1, "b": 2},
			expect:     int64(3),
		},
		{
			name:       "4",
			expression: "name",
			data:       map[string]any{"name": "test", "code": 123123},
			expect:     "test",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := Extract(c.expression, c.data)
			if err != nil {
				t.Fatal(err)
			}
			if res != c.expect {
				t.Fatalf("## extract fail. expect-[%v] but got-[%v]", c.expect, res)
			}
		})
	}
}

func BenchmarkExtract(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Extract("date_parse(date + left_pad(time, \"0\", 9), \"YYYYMMDDHHmmssSSS\")", map[string]any{"date": "20221123", "time": "94625100"})
	}
}
