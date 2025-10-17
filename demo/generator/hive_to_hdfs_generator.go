package generator

import (
	"fmt"
	"strings"
)

// HdfsSourceTable 代表 SELECT 查询的源表。
type HdfsSourceTable struct {
	Schema string
	Name   string
}

// FullName 返回带 schema 的完整表名。
func (t HdfsSourceTable) FullName() string {
	if t.Schema != "" {
		return fmt.Sprintf("%s.%s", t.Schema, t.Name)
	}
	return t.Name
}

// HdfsColumnMapping 定义了从源字段到别名的映射。
type HdfsColumnMapping struct {
	Expression string
	Alias      string
}

// HiveToHdfsScript 是一个专门用于描述 “Hive导出到HDFS” 脚本的对象。
// 它包含了动态生成脚本所需的所有变量。
type HiveToHdfsScript struct {
	HiveSettings    map[string]string
	DirectoryPath   string
	IsRowFormatSet  bool
	FieldTerminator string
	SelectColumns   []HdfsColumnMapping
	FromTable       HdfsSourceTable
	WhereClause     string // 虽然此步骤没有，但为保持模型完整性而包含
}

// Generate 方法根据对象中的变量动态构建（常量化）完整的脚本字符串。
func (h *HiveToHdfsScript) Generate() string {
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
	sb.WriteString(fmt.Sprintf("FROM %s", h.FromTable.FullName()))
	if h.WhereClause != "" {
		sb.WriteString(fmt.Sprintf("\nWHERE %s", h.WhereClause))
	}

	return sb.String()
}
