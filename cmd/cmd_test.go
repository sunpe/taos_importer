package main

import (
	"context"
	"testing"
)

func TestImportData(t *testing.T) {
	importData(context.Background(), "../config/conf.toml", nil, nil)
}
