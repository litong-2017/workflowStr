package generator

import (
	"fmt"
	"strings"
)

// StoredProcedureIncrCall 代表一个用于增量步骤的通用数据库存储过程调用。
type StoredProcedureIncrCall struct {
	ProcedureName string
	// 参数应作为字符串提供，例如 "my_table" 或 "null"。
	Arguments []string
}

// Generate 方法根据对象中的变量动态构建存储过程调用的字符串。
func (spc *StoredProcedureIncrCall) Generate() string {
	var processedArgs []string
	for _, arg := range spc.Arguments {
		// 在SQL中，'null' 是一个关键字，不应该被引号包围。
		if strings.ToLower(arg) == "null" {
			processedArgs = append(processedArgs, arg)
		} else {
			processedArgs = append(processedArgs, fmt.Sprintf("'%s'", arg))
		}
	}

	return fmt.Sprintf("%s(%s);", spc.ProcedureName, strings.Join(processedArgs, ", "))
}
