package db_table

import (
	"testing"
)

func TestDBManager_CreateDBSql(t *testing.T) {
	cases := []struct {
		name   string
		param  DBParam
		expect string
	}{
		{
			name: "1",
			param: DBParam{
				DBName: "test",
			},
			expect: "create database if not exists `test`",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := createDBSql(c.param)
			if err != nil {
				t.Fatal(err)
			}
			if res != c.expect {
				t.Log(res)
				t.Fatal(" create database sql error")
			}
		})
	}
}

func TestDBManager_CreateSTableSql(t *testing.T) {
	cases := []struct {
		name   string
		param  STableParam
		expect string
	}{
		{
			name: "1",
			param: STableParam{
				DBName:     "test",
				STableName: "meters",
				Columns: []TableColumn{
					{
						ColumnName: "ts",
						ColumnType: "timestamp",
					},
					{
						ColumnName: "current",
						ColumnType: "float",
					},
					{
						ColumnName: "voltage",
						ColumnType: "int",
					},
					{
						ColumnName: "phase",
						ColumnType: "float",
					},
				},
				Tags: []TableColumn{
					{
						ColumnName: "location",
						ColumnType: "varchar(64)",
					},
					{
						ColumnName: "groupid",
						ColumnType: "int",
					},
				},
			},
			expect: "create stable if not exists `meters` (`ts` timestamp, `current` float, `voltage` int, `phase` float) tags (`location` varchar(64), `groupid` int)",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if createSTableSql(c.param) != c.expect {
				t.Fatal("create stable sql error")
			}
		})
	}
}

func TestDBManager_CreateTableSql(t *testing.T) {
	cases := []struct {
		name   string
		param  TableParam
		expect string
	}{
		{
			name: "1",
			param: TableParam{
				TableName: "d0",
				Columns: []TableColumn{
					{
						ColumnName: "ts",
						ColumnType: "timestamp",
					},
					{
						ColumnName: "a",
						ColumnType: "int",
					},
				},
			},
			expect: "create table if not exists `d0` (`ts` timestamp, `a` int)",
		},
		{
			name: "2",
			param: TableParam{
				STableName: "stb1",
				TableName:  "d0",
				TagValues: []TagValue{
					{
						TagName:      "location",
						TagValue:     "California.SanFrancisco",
						TagValueType: "string",
					},
					{
						TagName:      "groupid",
						TagValue:     "1",
						TagValueType: "int",
					},
				},
			},
			expect: "create table if not exists `d0` using `stb1` (location, groupid) tags ('California.SanFrancisco', 1)",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if res := createTableSql(c.param); res != c.expect {
				t.Fatal("create table sql error")
			}
		})
	}
}
