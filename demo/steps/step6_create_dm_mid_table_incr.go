package steps

import "demo/generator"

// GetStep6CreateDmMidTableIncrConfig 创建并返回一个为第六步（创建达梦中间表-增量）
// 专门配置的 StoredProcedureIncrCall 对象。
func GetStep6CreateDmMidTableIncrConfig() *generator.StoredProcedureIncrCall {
	return &generator.StoredProcedureIncrCall{
		ProcedureName: "p_create_mid_app",
		// 存储过程的参数列表
		Arguments: []string{
			"T_APP_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR",
			"null",
		},
	}
}
