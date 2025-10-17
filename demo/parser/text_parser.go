package parser

import (
	"bufio"
	"demo/generator"
	"fmt"
	"regexp"
	"strings"
)

// DwsTable holds the parsed information from the text file for one DWS table.
type DwsTable struct {
	Name           string
	SourceSheet    string
	Remark         string
	IncrementField string
	Fields         []Field
}

// Field holds the parsed information for a single column.
type Field struct {
	Name        string
	Type        string
	SourceTable string
	Logic       string
	Remark      string
}

// ParseTablesFile parses the content of a tables.txt file and returns a slice of DwsTable objects.
func ParseTablesFile(content string) ([]*DwsTable, error) {
	var tables []*DwsTable
	var currentTable *DwsTable
	var isReadingFields bool

	scanner := bufio.NewScanner(strings.NewReader(content))

	reKeyValue := regexp.MustCompile(`^([\p{Han}\w\s]+):\s*(.*)`)
	reFieldHeader := regexp.MustCompile(`^\[字段 (\d+)]`)
	reFieldName := regexp.MustCompile(`^\s+字段名:\s*(.*)`)
	reFieldLogic := regexp.MustCompile(`^\s+字段逻辑:\s*(.*)`)
	reFieldSource := regexp.MustCompile(`^\s+来源表:\s*(.*)`)

	for scanner.Scan() {
		line := scanner.Text()
		trimmedLine := strings.TrimSpace(line)

		if strings.HasPrefix(trimmedLine, "【DWS表") {
			if currentTable != nil && currentTable.Name != "" {
				tables = append(tables, currentTable)
			}
			currentTable = &DwsTable{}
			isReadingFields = false
		} else if currentTable != nil {
			if strings.HasPrefix(trimmedLine, "字段详情:") {
				isReadingFields = true
				continue
			}

			if isReadingFields {
				if reFieldHeader.MatchString(trimmedLine) {
					currentTable.Fields = append(currentTable.Fields, Field{})
				} else if len(currentTable.Fields) > 0 {
					lastField := &currentTable.Fields[len(currentTable.Fields)-1]
					if matches := reFieldName.FindStringSubmatch(line); len(matches) > 1 {
						lastField.Name = strings.TrimSpace(matches[1])
					} else if matches := reFieldLogic.FindStringSubmatch(line); len(matches) > 1 {
						lastField.Logic = strings.TrimSpace(matches[1])
					} else if matches := reFieldSource.FindStringSubmatch(line); len(matches) > 1 {
						lastField.SourceTable = strings.TrimSpace(matches[1])
					}
				}
			} else {
				if matches := reKeyValue.FindStringSubmatch(trimmedLine); len(matches) > 2 {
					key, value := strings.TrimSpace(matches[1]), strings.TrimSpace(matches[2])
					switch key {
					case "表英文名":
						currentTable.Name = value
					case "事实表详情Sheet页名":
						currentTable.SourceSheet = value
					case "备注":
						currentTable.Remark = value
					case "增量字段":
						currentTable.IncrementField = value
					}
				}
			}
		}
	}
	if currentTable != nil && len(currentTable.Name) > 0 {
		tables = append(tables, currentTable)
	}

	return tables, nil
}

func (dt *DwsTable) ToHiveSQLConfig() *generator.HiveInitializationSQL {
	config := &generator.HiveInitializationSQL{}
	tableAliases := make(map[string]string)
	fromTableAlias := "s"

	// Pass 1: Identify tables and assign aliases
	config.TargetTable = generator.Table{Schema: "dws", Name: dt.Name}
	joinTables := make(map[string]struct{})
	var fromTableSet bool
	for _, field := range dt.Fields {
		sourceTable := field.SourceTable
		if sourceTable == "" {
			continue
		}

		lowerSourceTable := strings.ToLower(sourceTable)
		if strings.HasPrefix(lowerSourceTable, "t_dwd") && !fromTableSet {
			config.FromTable = generator.Table{Schema: "dwd", Name: sourceTable, Alias: fromTableAlias}
			tableAliases[sourceTable] = fromTableAlias
			fromTableSet = true
		} else if strings.HasPrefix(lowerSourceTable, "t_dim") {
			joinTables[sourceTable] = struct{}{}
		}
	}

	if !fromTableSet { // Fallback
		for _, field := range dt.Fields {
			if field.SourceTable != "" {
				config.FromTable = generator.Table{Schema: dt.getSchemaForTable(field.SourceTable), Name: field.SourceTable, Alias: fromTableAlias}
				tableAliases[field.SourceTable] = fromTableAlias
				break
			}
		}
	}

	if config.FromTable.Name == "" {
		return nil
	}

	// Pass 2: Create JOIN clauses
	joinAliasCounter := 0
	for tableName := range joinTables {
		if _, exists := tableAliases[tableName]; exists {
			continue
		}
		alias := string(rune('a' + joinAliasCounter))
		tableAliases[tableName] = alias
		config.Joins = append(config.Joins, generator.Join{
			Type:      "left join",
			Target:    generator.Table{Schema: "dim", Name: tableName, Alias: alias},
			Condition: fmt.Sprintf("%s.fk_id = %s.pk_id", fromTableAlias, alias),
			IsActive:  true,
		})
		joinAliasCounter++
	}

	// === Pass 3 (NEW): Pre-scan to find all raw columns used in aggregations ===
	aggregatedRawColumns := getAggregatedRawColumns(dt.Fields)

	// Pass 4: Build SelectColumns and GroupByColumns
	var groupByCols []generator.GroupByColumn
	hasAggregation := len(aggregatedRawColumns) > 0

	for _, field := range dt.Fields {
		expression := dt.buildAliasedExpression(field, tableAliases, fromTableAlias)

		config.SelectColumns = append(config.SelectColumns, generator.ColumnMapping{
			Expression: expression,
			Alias:      field.Name,
		})

		// A field is added to GROUP BY if:
		// 1. The query has aggregations.
		// 2. The field's own logic is NOT an aggregate function.
		// 3. The field's own logic is NOT a constant.
		// 4. The field's raw logic is NOT in the set of pre-scanned aggregated columns.
		_, isAggregatedRaw := aggregatedRawColumns[field.Logic]
		if hasAggregation && !isAggregate(field.Logic) && !isConstant(expression) && !isAggregatedRaw {
			groupByCols = append(groupByCols, generator.GroupByColumn{Expression: expression, IsActive: true})
		}
	}

	if hasAggregation {
		config.GroupByColumns = groupByCols
	}

	// Pass 5: Set Where Clause
	if dt.IncrementField != "" {
		config.WhereClause = fmt.Sprintf("%s.%s >= date_sub(current_date, 1)", fromTableAlias, dt.IncrementField)
	}

	return config
}

// NEW helper function to pre-scan for raw columns inside aggregate functions
func getAggregatedRawColumns(fields []Field) map[string]struct{} {
	rawCols := make(map[string]struct{})
	reExtract := regexp.MustCompile(`\((.*)\)`)
	reLastWord := regexp.MustCompile(`(\S+)$`)

	for _, field := range fields {
		if isAggregate(field.Logic) {
			matches := reExtract.FindStringSubmatch(field.Logic)
			if len(matches) > 1 {
				innerArgs := matches[1]
				rawColName := reLastWord.FindString(innerArgs)
				if rawColName != "" {
					rawCols[rawColName] = struct{}{}
				}
			}
		}
	}
	return rawCols
}

func (dt *DwsTable) buildAliasedExpression(field Field, aliases map[string]string, defaultAlias string) string {
	logic := strings.TrimSpace(field.Logic)
	if logic == "" {
		return "''"
	}

	// Get the correct alias for the current field's source table
	sourceAlias := defaultAlias
	if field.SourceTable != "" {
		if alias, ok := aliases[field.SourceTable]; ok {
			sourceAlias = alias
		}
	}

	// Regex to find a function call, e.g., "sum(SEG_PRICE_TPM)" or "count(distinct TKT_NUM)"
	reFunc := regexp.MustCompile(`^(\w+)\s*\((.*)\)$`)
	matches := reFunc.FindStringSubmatch(logic)

	if len(matches) == 3 {
		funcName := matches[1]
		innerArgs := strings.TrimSpace(matches[2])

		reLastWord := regexp.MustCompile(`(\S+)$`)
		columnName := reLastWord.FindString(innerArgs)

		if columnName != "" {
			aliasedColumn := sourceAlias + "." + columnName
			innerArgs = reLastWord.ReplaceAllString(innerArgs, aliasedColumn)
		}

		return fmt.Sprintf("%s(%s)", funcName, innerArgs)
	}

	if regexp.MustCompile(`^[a-zA-Z0-9_]+$`).MatchString(logic) {
		return sourceAlias + "." + logic
	}

	return logic
}

func (dt *DwsTable) getSchemaForTable(tableName string) string {
	lowerName := strings.ToLower(tableName)
	if strings.HasPrefix(lowerName, "t_dwd") {
		return "dwd"
	}
	if strings.HasPrefix(lowerName, "t_dim") {
		return "dim"
	}
	return "default"
}

func isAggregate(s string) bool {
	lower := strings.ToLower(s)
	return strings.HasPrefix(lower, "sum(") || strings.HasPrefix(lower, "count(") || strings.HasPrefix(lower, "avg(") || strings.HasPrefix(lower, "min(") || strings.HasPrefix(lower, "max(")
}

func isConstant(logic string) bool {
	logic = strings.TrimSpace(logic)
	if (strings.HasPrefix(logic, "'") && strings.HasSuffix(logic, "'")) ||
		(strings.HasPrefix(logic, "\"") && strings.HasSuffix(logic, "\"")) {
		return true
	}
	if strings.Contains(logic, "current_timestamp") || strings.Contains(logic, "current_date") {
		return true
	}
	return false
}
