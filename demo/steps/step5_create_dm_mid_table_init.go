package steps

import "demo/generator"

// GetStep5CreateDmMidTableInitConfig 创建并返回一个为第五步（创建达梦中间表-初始化）
// 专门配置的 StoredProcedureCall 对象。
func GetStep5CreateDmMidTableInitConfig() *generator.StoredProcedureCall {
	return &generator.StoredProcedureCall{
		ProcedureName: "p_create_mid_app",
		// 存储过程的参数列表
		Arguments: []string{
			"T_APP_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR",
			"null",
		},
	}
}
