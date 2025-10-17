package generator

import (
	"fmt"
	"strings"
)

// IncrTable 代表增量查询中的一个数据库表。
type IncrTable struct {
	Schema string
	Name   string
	Alias  string
}

// FullName 返回带 schema 的完整表名。
func (t IncrTable) FullName() string {
	if t.Schema != "" {
		return fmt.Sprintf("%s.%s", t.Schema, t.Name)
	}
	return t.Name
}

// IncrColumnMapping 定义了从源表达式到目标列的映射。
type IncrColumnMapping struct {
	Expression string
	Alias      string
}

// IncrJoin 代表一个 JOIN 子句。
type IncrJoin struct {
	Type      string
	Target    IncrTable
	Condition string
	IsActive  bool
}

// IncrGroupByColumn 代表 GROUP BY 子句中的一个字段。
type IncrGroupByColumn struct {
	Expression string
	IsActive   bool
}

// HiveIncrementalSQL 是一个专门用于描述增量 Hive SQL 查询的 Go 对象。
// 它包含了动态生成查询所需的所有变量。
type HiveIncrementalSQL struct {
	TargetTable     IncrTable
	PartitionClause string
	SelectColumns   []IncrColumnMapping
	FromTable       IncrTable
	Joins           []IncrJoin
	WhereClause     string
	GroupByColumns  []IncrGroupByColumn
}

// Generate 方法根据对象中的变量动态构建（常量化）增量 SQL 查询字符串。
func (h *HiveIncrementalSQL) Generate() string {
	var sb strings.Builder

	// INSERT 子句
	sb.WriteString("insert overwrite table ")
	sb.WriteString(h.TargetTable.FullName())
	if h.PartitionClause != "" {
		sb.WriteString(" partition(")
		sb.WriteString(h.PartitionClause)
		sb.WriteString(")")
	}
	sb.WriteString("\nselect\n")

	// SELECT 子句
	for i, col := range h.SelectColumns {
		if i > 0 {
			sb.WriteString(",\n")
		}
		sb.WriteString(fmt.Sprintf("    %s", col.Expression))
		if col.Alias != "" {
			sb.WriteString(" as ")
			sb.WriteString(col.Alias)
		}
	}
	sb.WriteString("\n")

	// FROM 子句
	sb.WriteString("from ")
	sb.WriteString(h.FromTable.FullName())
	sb.WriteString(" ")
	sb.WriteString(h.FromTable.Alias)
	sb.WriteString("\n")

	// JOIN 子句
	for _, j := range h.Joins {
		if !j.IsActive {
			sb.WriteString("--")
		}
		sb.WriteString(j.Type)
		sb.WriteString(" ")
		sb.WriteString(j.Target.FullName())
		sb.WriteString(" ")
		sb.WriteString(j.Target.Alias)
		sb.WriteString(" on ")
		sb.WriteString(j.Condition)
		sb.WriteString("\n")
	}

	// WHERE 子句
	if h.WhereClause != "" {
		sb.WriteString("where ")
		sb.WriteString(h.WhereClause)
		sb.WriteString("\n")
	}

	// GROUP BY 子句
	sb.WriteString("group by\n")
	isFirstActive := true
	for _, col := range h.GroupByColumns {
		var line string
		if col.IsActive {
			if isFirstActive {
				line = fmt.Sprintf("    %s", col.Expression)
				isFirstActive = false
			} else {
				line = fmt.Sprintf("    , %s", col.Expression)
			}
		} else {
			line = fmt.Sprintf("    --, %s", col.Expression)
		}
		sb.WriteString(line)
		sb.WriteString("\n")
	}

	return sb.String()
}
