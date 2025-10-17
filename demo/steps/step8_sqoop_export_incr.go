package steps

import "demo/generator"

// GetStep8SqoopExportIncrConfig 创建并返回一个 SqoopExportIncrCommand 对象，
// 该对象专门为第八步（数据载入达梦临时表 - 增量）进行了配置。
func GetStep8SqoopExportIncrConfig() *generator.SqoopExportIncrCommand {
	return &generator.SqoopExportIncrCommand{
		SqoopPath: "/usr/bch/3.3.0/sqoop/bin/sqoop",
		Command:   "export",
		Arguments: map[string]string{
			"options-file": "/usr/bch/3.3.0/sqoop/conf/dm8_pro.props",
			"table":        "MID_T_APP_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR",
			"export-dir":   "/tmp/hive/hive/T_DWS_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR",
			"num-mappers":  "8",
		},
		Flags: []string{
			"batch",
		},
	}
}
