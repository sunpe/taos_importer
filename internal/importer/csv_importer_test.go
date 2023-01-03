package importer

import (
	"context"
	"taos_importer/internal/config"
	"testing"
)

func TestCsvImporter_Import(t *testing.T) {

	conf := config.Config{
		TDEngine: config.TDEngine{
			Host:     "localhost",
			Port:     6030,
			User:     "root",
			Password: "taosdata",
		},
		DB: config.Database{
			Name:      "test",
			Precision: "us",
		},
		STable: config.STable{
			Columns: []config.Column{
				{
					Field:  "ts",
					Type:   "timestamp",
					Source: "avoid_datetime_conflict(date_parse(date + left_pad(time, \"0\", 9), \"YYYYMMDDHHmmssSSS\"), 100)",
				},
				{
					Field: "code",
					Type:  "int",
				},
				{
					Field: "wind_code",
					Type:  "varchar(10)",
				},
				{
					Field: "name",
					Type:  "nchar(10)",
				},
				{
					Field: "function_code",
					Type:  "int",
				},
				{
					Field: "order_kind",
					Type:  "int",
				},
				{
					Field: "bs_flag",
					Type:  "int",
				},
				{
					Field: "trade_price",
					Type:  "int",
				},
				{
					Field: "trade_volume",
					Type:  "int",
				},
				{
					Field: "ask_order",
					Type:  "int",
				},
				{
					Field: "bid_order",
					Type:  "int",
				},
				{
					Field: "channel",
					Type:  "int",
				},
				{
					Field: "index",
					Type:  "int",
				},
				{
					Field: "biz_index",
					Type:  "int",
				},
			},
		},
		Concurrent: 10,
		BatchSize:  100,
	}
	c, err := NewCsvImporter(conf, "t_900957")
	if err != nil {
		panic(err)
	}

	err = c.Import(context.Background(), "/Users/sunpeng/workspace/tmp/taos/data/900957.csv")
	if err != nil {
		panic(err)
	}
	t.Log("## total ", c.Total.Load())
	t.Log("## error ", c.ErrorCount.Load())
	t.Log("## spend ", c.End.Sub(c.Start).Milliseconds())
}
