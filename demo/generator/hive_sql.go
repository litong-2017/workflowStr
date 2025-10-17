package generator

import (
	"fmt"
	"strings"
)

// Table represents a database table with an optional schema and alias.
type Table struct {
	Schema string `json:"schema,omitempty"` // e.g., "dws", "dwd"
	Name   string `json:"name"`             // e.g., "T_DWS_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR"
	Alias  string `json:"alias,omitempty"`  // e.g., "s", "da"
}

// FullName returns the schema-qualified table name (e.g., "dws.T_MY_TABLE").
func (t Table) FullName() string {
	if t.Schema != "" {
		return fmt.Sprintf("%s.%s", t.Schema, t.Name)
	}
	return t.Name
}

// ColumnMapping defines the mapping from a source expression to a target column alias.
type ColumnMapping struct {
	Expression string `json:"expression"` // e.g., "s.SALE_DATE", "CASE WHEN ... END"
	Alias      string `json:"alias"`      // e.g., "SELL_DATE"
}

// Join represents a join clause in a SQL query.
type Join struct {
	Type      string `json:"type"`      // e.g., "left join"
	Target    Table  `json:"target"`    // The table to join with
	Condition string `json:"condition"` // The ON condition, e.g., "s.SALE_DATE = da.pk_id"
	IsActive  bool   `json:"isActive"`  // Flag to enable or disable the join (for commented-out code)
}

// GroupByColumn represents a column in the GROUP BY clause.
type GroupByColumn struct {
	Expression string `json:"expression"`
	IsActive   bool   `json:"isActive"`
}

// HiveInitializationSQL holds all the structured components of the initialization SQL query.
// It acts as a configurable blueprint for generating the query.
type HiveInitializationSQL struct {
	TargetTable    Table
	SelectColumns  []ColumnMapping
	FromTable      Table
	Joins          []Join
	WhereClause    string
	GroupByColumns []GroupByColumn
}

// GenerateSQL dynamically constructs the Hive SQL query from the object's properties.
// This method acts as the "constant" part, defining the query's structure.
func (h *HiveInitializationSQL) GenerateSQL() string {
	var sb strings.Builder

	// INSERT clause
	sb.WriteString("insert overwrite table ")
	sb.WriteString(h.TargetTable.FullName())
	sb.WriteString("\nselect\n")

	// SELECT columns
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

	// FROM clause
	sb.WriteString("from ")
	sb.WriteString(h.FromTable.FullName())
	sb.WriteString(" ")
	sb.WriteString(h.FromTable.Alias)
	sb.WriteString("\n")

	// JOIN clauses
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

	// WHERE clause
	if h.WhereClause != "" {
		sb.WriteString("where ")
		sb.WriteString(h.WhereClause)
		sb.WriteString("\n")
	}

	// GROUP BY clause
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
		if col.IsActive || col.IsActive == false {
			sb.WriteString("\n")
		}
	}

	return sb.String()
}
