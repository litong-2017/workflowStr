package generator

import (
	"fmt"
	"strings"
)

// IncrHdfsSourceTable 代表增量导出中 SELECT 查询的源表。
type IncrHdfsSourceTable struct {
	Schema string
	Name   string
}

// FullName 返回带 schema 的完整表名。
func (t IncrHdfsSourceTable) FullName() string {
	if t.Schema != "" {
		return fmt.Sprintf("%s.%s", t.Schema, t.Name)
	}
	return t.Name
}

// IncrHdfsColumnMapping 定义了从源字段到别名的映射。
type IncrHdfsColumnMapping struct {
	Expression string
	Alias      string
}

// HiveToHdfsIncrScript 是一个专门描述“Hive增量导出到HDFS”脚本的对象。
// 它包含了动态生成脚本所需的所有变量。
type HiveToHdfsIncrScript struct {
	HiveSettings    map[string]string
	DirectoryPath   string
	IsRowFormatSet  bool
	FieldTerminator string
	SelectColumns   []IncrHdfsColumnMapping
	FromTable       IncrHdfsSourceTable
	WhereClause     string
}

// Generate 方法根据对象中的变量动态构建（常量化）完整的增量脚本字符串。
func (h *HiveToHdfsIncrScript) Generate() string {
	var sb strings.Builder

	// 1. 生成 SET 命令
	for key, value := range h.HiveSettings {
		sb.WriteString(fmt.Sprintf("SET %s=%s;\n", key, value))
	}

	// 2. 生成 INSERT OVERWRITE DIRECTORY
	sb.WriteString(fmt.Sprintf("INSERT OVERWRITE DIRECTORY '%s'\n", h.DirectoryPath))

	// 3. 生成 ROW FORMAT
	if h.IsRowFormatSet {
		sb.WriteString("ROW FORMAT DELIMITED\n")
		sb.WriteString(fmt.Sprintf("FIELDS TERMINATED BY '%s'\n", h.FieldTerminator))
	}

	// 4. 生成 SELECT 子句
	sb.WriteString("SELECT\n")
	for i, col := range h.SelectColumns {
		line := fmt.Sprintf("  %s", col.Expression)
		if col.Alias != "" {
			line += fmt.Sprintf("  AS %s", col.Alias)
		}
		if i < len(h.SelectColumns)-1 {
			line += ","
		}
		sb.WriteString(line)
		sb.WriteString("\n")
	}

	// 5. 生成 FROM 子句
	sb.WriteString(fmt.Sprintf("FROM %s\n", h.FromTable.FullName()))

	// 6. 生成 WHERE 子句
	if h.WhereClause != "" {
		sb.WriteString(fmt.Sprintf("WHERE %s", h.WhereClause))
	}

	return sb.String()
}
