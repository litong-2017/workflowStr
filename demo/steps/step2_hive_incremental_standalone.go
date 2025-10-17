package steps

import "demo/generator"

// GetStep2HiveIncrementalStandaloneConfig 创建并返回一个独立的、为第二步（增量）专门配置的 Go 对象。
func GetStep2HiveIncrementalStandaloneConfig() *generator.HiveIncrementalSQL {
	return &generator.HiveIncrementalSQL{
		TargetTable: generator.IncrTable{
			Schema: "dws",
			Name:   "T_DWS_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR",
		},
		PartitionClause: "dt='${mt1}'",
		FromTable: generator.IncrTable{
			Schema: "dwd",
			Name:   "T_DWD_SA_INTERNAT_TICKING_FLYR_FACT",
			Alias:  "s",
		},
		SelectColumns: []generator.IncrColumnMapping{
			{Expression: "date_format(current_timestamp, 'yyyyMMddHHmmss')", Alias: "ETL_TIME"},
			{Expression: "s.SALE_DATE", Alias: "SELL_DATE"},
			{Expression: "da.flt_week", Alias: "SELL_WEEK"},
			{Expression: "s.SALE_MONTH", Alias: "SELL_YM"},
			{Expression: "SUBSTR(s.SALE_MONTH,0,4)", Alias: "SELL_Y"},
			{Expression: "s.TKT_VOYAGE", Alias: "VOYAGE"},
			{Expression: "''", Alias: "BUS_DEP"},
			{Expression: "s.TKT_AIR_NAME", Alias: "ISS_AIR_NAME"},
			{Expression: "s.VOYAGE_TYPE", Alias: "DAF_MARK"},
			{Expression: "s.DIRDIS", Alias: "DIRDIS_MARK"},
			{Expression: "s.ABROAD_AIRPORT_COUNTRY", Alias: "ABROAD_AIRPORT_AREA"},
			{Expression: "s.IS_LOCPS_AIRPORT_DEP", Alias: "IS_LOCPS_AIRPORT_DEP"},
			{Expression: "s.IS_LOCPS_AIRPORT", Alias: "IS_LOCPS_AIRPORT"},
			{Expression: "CASE WHEN s.GRP_FIT_MARK='G' then '团队' else '散客' end", Alias: "TEAM_MARK"},
			{Expression: "s.VOYAGE_MARK", Alias: "VOYAGE_MARK"},
			{Expression: "s.CHN_AREA", Alias: "CHN_AREA"},
			{Expression: "s.CHN_NATURE", Alias: "CHN_NATURE"},
			{Expression: "s.CHN_DETAIL1", Alias: "CHN_DETAIL_1"},
			{Expression: "s.CHN_DETAIL2", Alias: "CHN_DETAIL_2"},
			{Expression: "sum(s.INCOME_VOYAGE)", Alias: "SALE_AMT"},
			{Expression: "count(distinct s.TKT_NUM)", Alias: "SALE_NUM"},
			{Expression: "max_pt('dwd','T_DWD_SA_INTERNAT_TICKING_FLYR_FACT')", Alias: ""},
		},
		Joins: []generator.IncrJoin{
			{
				Type: "left join",
				Target: generator.IncrTable{
					Schema: "dim",
					Name:   "T_DIM_DATE",
					Alias:  "da",
				},
				Condition: "s.SALE_DATE =da.pk_id",
				IsActive:  true,
			},
			{
				Type: "left join",
				Target: generator.IncrTable{
					Schema: "dim",
					Name:   "t_dim_agent",
					Alias:  "ag",
				},
				Condition: "s.fk_tkt_agent_id =ag.pk_id and ag.dt=max_pt('dim','t_dim_agent')",
				IsActive:  false, // 此 JOIN 在原始查询中被注释
			},
		},
		WhereClause: `s.dt='${mt1}' and s.DATA_MONTH<='${mt1}'
    AND s.DATA_MONTH>=date_format(add_months(trunc(from_unixtime(unix_timestamp('${mt1}','yyyyMM'),'yyyy-MM'),'MM'),-1),'yyyyMM')`,
		GroupByColumns: []generator.IncrGroupByColumn{
			{Expression: "s.SALE_DATE", IsActive: true},
			{Expression: "da.flt_week", IsActive: true},
			{Expression: "s.SALE_MONTH", IsActive: true},
			{Expression: "s.TKT_VOYAGE", IsActive: true},
			{Expression: "ag.AGENT_BUS_DEP_3_CODE", IsActive: false}, // 此字段在原始查询中被注释
			{Expression: "s.TKT_AIR_NAME", IsActive: true},
			{Expression: "s.VOYAGE_TYPE", IsActive: true},
			{Expression: "s.DIRDIS", IsActive: true},
			{Expression: "s.ABROAD_AIRPORT_COUNTRY", IsActive: true},
			{Expression: "s.IS_LOCPS_AIRPORT_DEP", IsActive: true},
			{Expression: "s.IS_LOCPS_AIRPORT", IsActive: true},
			{Expression: "s.GRP_FIT_MARK", IsActive: true},
			{Expression: "s.VOYAGE_MARK", IsActive: true},
			{Expression: "s.CHN_AREA", IsActive: true},
			{Expression: "s.CHN_NATURE", IsActive: true},
			{Expression: "s.CHN_DETAIL1", IsActive: true},
			{Expression: "s.CHN_DETAIL2", IsActive: true},
		},
	}
}
