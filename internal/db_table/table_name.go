package db_table

import (
	"crypto/md5"
	"encoding/hex"
	"sort"
	"taos_importer/internal/common"
	"taos_importer/internal/field"
)

func GenerateTableName(pattern string, tags map[string]any) (string, error) {
	if len(pattern) == 0 {
		return generateTableNameByTags(tags), nil
	}

	tableName, err := field.Extract(pattern, tags)
	return common.String(tableName), err
}

func generateTableNameByTags(tags map[string]any) string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	m := md5.New()
	for _, k := range keys {
		m.Write([]byte(common.String(tags[k])))
	}

	return "t_" + hex.EncodeToString(m.Sum(nil))
}
