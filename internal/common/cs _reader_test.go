package common

import (
	"testing"
)

func TestReadCsv(t *testing.T) {
	path := "/Users/sunpeng/workspace/tmp/taos/data/900957.csv"
	ch, err := ReadCsv(path)
	if err != nil {
		t.Fatal(err)
	}
	for data := range ch {
		t.Log(data)
	}
}
