package steps

import "demo/generator"

// GetStep4HiveToHdfsIncrConfig 创建并返回一个为第四步（Hive增量导出到HDFS）专门配置的 Go 对象。
func GetStep4HiveToHdfsIncrConfig() *generator.HiveToHdfsIncrScript {
	return &generator.HiveToHdfsIncrScript{
		HiveSettings: map[string]string{
			"hive.exec.compress.output":                        "true",
			"mapreduce.output.fileoutputformat.compress.codec": "org.apache.hadoop.io.compress.SnappyCodec",
			"mapreduce.output.fileoutputformat.compress.type":  "BLOCK",
		},
		DirectoryPath:   "/tmp/hive/hive/T_DWS_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR",
		IsRowFormatSet:  true,
		FieldTerminator: ",",
		SelectColumns: []generator.IncrHdfsColumnMapping{
			{Expression: "date_format(current_timestamp, 'yyyyMMddHHmmss')", Alias: "ETL_TIME"},
			{Expression: "dt", Alias: "DATA_MONTH"},
			{Expression: "from_unixtime(unix_timestamp(SELL_DATE,'yyyyMMdd'),'yyyy-MM-dd')", Alias: "SELL_DATE"},
			{Expression: "SELL_WEEK", Alias: ""},
			{Expression: "SELL_YM", Alias: ""},
			{Expression: "SELL_Y", Alias: ""},
			{Expression: "VOYAGE", Alias: ""},
			{Expression: "BUS_DEP", Alias: ""},
			{Expression: "ISS_AIR_NAME", Alias: ""},
			{Expression: "DAF_MARK", Alias: ""},
			{Expression: "DIRDIS_MARK", Alias: ""},
			{Expression: "ABROAD_AIRPORT_AREA", Alias: ""},
			{Expression: "IS_LOCPS_AIRPORT_DEP", Alias: ""},
			{Expression: "IS_LOCPS_AIRPORT", Alias: ""},
			{Expression: "TEAM_MARK", Alias: ""},
			{Expression: "VOYAGE_MARK", Alias: ""},
			{Expression: "CHN_AREA", Alias: ""},
			{Expression: "CHN_NATURE", Alias: ""},
			{Expression: "CHN_DETAIL_1", Alias: ""},
			{Expression: "CHN_DETAIL_2", Alias: ""},
			{Expression: "SALE_AMT", Alias: ""},
			{Expression: "SALE_NUM", Alias: ""},
		},
		FromTable: generator.IncrHdfsSourceTable{
			Schema: "DWS",
			Name:   "T_DWS_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR",
		},
		WhereClause: `dt = '${mt1}' and DATA_MONTH<='${mt1}'
    AND DATA_MONTH>=date_format(add_months(trunc(from_unixtime(unix_timestamp('${mt1}','yyyyMM'),'yyyy-MM'),'MM'),-1),'yyyyMM')`,
	}
}
