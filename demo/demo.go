package main

// TableName represents a SQL table with its schema, name, and alias.
type TableName struct {
	Schema string
	Name   string
	Alias  string
}

// FieldMapping represents a field in a SELECT clause, including its source and alias.
type FieldMapping struct {
	SourceExpression string
	Alias            string
}

// JoinClause represents a JOIN operation in a SQL query.
type JoinClause struct {
	Type        string
	Table       TableName
	OnCondition string
}

// HiveSQLScript represents a structured Hive SQL query for dynamic generation.
type HiveSQLScript struct {
	ResultTable   TableName
	SelectFields  []FieldMapping
	FromTable     TableName
	Joins         []JoinClause
	WhereClause   string
	GroupByFields []string
}

// Step represents a single, individual action within an ETL process.
// The Script field can hold either a simple string or a structured object like HiveSQLScript.
type Step struct {
	ID          int
	Name        string
	Type        string // e.g., "initialization", "incremental"
	CommandType string // e.g., "hivesql", "shell", "dm_proc"
	Script      interface{}
}

// ETLProcess represents a complete sequence of steps for a single ETL job.
type ETLProcess struct {
	Name  string
	Steps []Step
}

// ETLBatch represents a collection of multiple ETL processes.
type ETLBatch struct {
	Processes []ETLProcess
}

// DemoProcess is the specific ETL process defined in demo.txt.
var DemoProcess = ETLProcess{
	Name: "T_DWS_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR",
	Steps: []Step{
		{
			ID:          1,
			Name:        "hive sql",
			Type:        "initialization",
			CommandType: "hivesql",
			Script: HiveSQLScript{
				ResultTable: TableName{Schema: "dws", Name: "T_DWS_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR"},
				SelectFields: []FieldMapping{
					{SourceExpression: "date_format(current_timestamp, 'yyyyMMddHHmmss')", Alias: "ETL_TIME"},
					{SourceExpression: "s.SALE_DATE", Alias: "SELL_DATE"},
					{SourceExpression: "da.flt_week", Alias: "SELL_WEEK"},
					{SourceExpression: "s.SALE_MONTH", Alias: "SELL_YM"},
					{SourceExpression: "SUBSTR(s.SALE_MONTH,0,4)", Alias: "SELL_Y"},
					{SourceExpression: "s.TKT_VOYAGE", Alias: "VOYAGE"},
					{SourceExpression: "''", Alias: "BUS_DEP"},
					{SourceExpression: "s.TKT_AIR_NAME", Alias: "ISS_AIR_NAME"},
					{SourceExpression: "s.VOYAGE_TYPE", Alias: "DAF_MARK"},
					{SourceExpression: "s.DIRDIS", Alias: "DIRDIS_MARK"},
					{SourceExpression: "s.ABROAD_AIRPORT_COUNTRY", Alias: "ABROAD_AIRPORT_AREA"},
					{SourceExpression: "s.IS_LOCPS_AIRPORT_DEP", Alias: "IS_LOCPS_AIRPORT_DEP"},
					{SourceExpression: "s.IS_LOCPS_AIRPORT", Alias: "IS_LOCPS_AIRPORT"},
					{SourceExpression: "CASE WHEN s.GRP_FIT_MARK='G' then '团队' else '散客' end", Alias: "TEAM_MARK"},
					{SourceExpression: "s.VOYAGE_MARK", Alias: "VOYAGE_MARK"},
					{SourceExpression: "s.CHN_AREA", Alias: "CHN_AREA"},
					{SourceExpression: "s.CHN_NATURE", Alias: "CHN_NATURE"},
					{SourceExpression: "s.CHN_DETAIL1", Alias: "CHN_DETAIL_1"},
					{SourceExpression: "s.CHN_DETAIL2", Alias: "CHN_DETAIL_2"},
					{SourceExpression: "sum(s.INCOME_VOYAGE)", Alias: "SALE_AMT"},
					{SourceExpression: "count(distinct s.TKT_NUM)", Alias: "SALE_NUM"},
					{SourceExpression: "max_pt('dwd','T_DWD_SA_INTERNAT_TICKING_FLYR_FACT')"},
				},
				FromTable: TableName{Schema: "dwd", Name: "T_DWD_SA_INTERNAT_TICKING_FLYR_FACT", Alias: "s"},
				Joins: []JoinClause{
					{
						Type:        "left join",
						Table:       TableName{Schema: "dim", Name: "T_DIM_DATE", Alias: "da"},
						OnCondition: "s.SALE_DATE = da.pk_id",
					},
				},
				WhereClause: "s.dt=max_pt('dwd','T_DWD_SA_INTERNAT_TICKING_FLYR_FACT')",
				GroupByFields: []string{
					"s.SALE_DATE", "da.flt_week", "s.SALE_MONTH", "s.TKT_VOYAGE",
					"s.TKT_AIR_NAME", "s.VOYAGE_TYPE", "s.DIRDIS", "s.ABROAD_AIRPORT_COUNTRY",
					"s.IS_LOCPS_AIRPORT_DEP", "s.IS_LOCPS_AIRPORT", "s.GRP_FIT_MARK",
					"s.VOYAGE_MARK", "s.CHN_AREA", "s.CHN_NATURE", "s.CHN_DETAIL1", "s.CHN_DETAIL2",
				},
			},
		},
		{
			ID:          2,
			Name:        "hive sql",
			Type:        "incremental",
			CommandType: "hivesql",
			Script: `insert overwrite table dws.T_DWS_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR partition(dt='${mt1}')
select
date_format(current_timestamp, 'yyyyMMddHHmmss') as ETL_TIME
,s.SALE_DATE as SELL_DATE
,da.flt_week as SELL_WEEK
,s.SALE_MONTH as SELL_YM
,SUBSTR(s.SALE_MONTH,0,4) as SELL_Y
,s.TKT_VOYAGE as VOYAGE
--,ag.AGENT_BUS_DEP_3_CODE as BUS_DEP
,'' as BUS_DEP
,s.TKT_AIR_NAME as ISS_AIR_NAME
,s.VOYAGE_TYPE as DAF_MARK
,s.DIRDIS as DIRDIS_MARK
,s.ABROAD_AIRPORT_COUNTRY as ABROAD_AIRPORT_AREA
,s.IS_LOCPS_AIRPORT_DEP as IS_LOCPS_AIRPORT_DEP
,s.IS_LOCPS_AIRPORT as IS_LOCPS_AIRPORT
,CASE WHEN s.GRP_FIT_MARK='G' then '团队' else '散客' end as TEAM_MARK
,s.VOYAGE_MARK as VOYAGE_MARK
,s.CHN_AREA as CHN_AREA
,s.CHN_NATURE as CHN_NATURE
,s.CHN_DETAIL1 as CHN_DETAIL_1
,s.CHN_DETAIL2 as CHN_DETAIL_2
,sum(s.INCOME_VOYAGE) as SALE_AMT
,count(distinct s.TKT_NUM) as SALE_NUM
,max_pt('dwd','T_DWD_SA_INTERNAT_TICKING_FLYR_FACT')
from dwd.T_DWD_SA_INTERNAT_TICKING_FLYR_FACT s
left join dim.T_DIM_DATE da on s.SALE_DATE =da.pk_id
--left join dim.t_dim_agent ag on s.fk_tkt_agent_id =ag.pk_id and ag.dt=max_pt('dim','t_dim_agent')
where s.dt='${mt1}' and s.DATA_MONTH<='${mt1}'
    AND s.DATA_MONTH>=date_format(add_months(trunc(from_unixtime(unix_timestamp('${mt1}','yyyyMM'),'yyyy-MM'),'MM'),-1),'yyyyMM');
group by
s.SALE_DATE
,da.flt_week
,s.SALE_MONTH
,s.TKT_VOYAGE
--,ag.AGENT_BUS_DEP_3_CODE
,s.TKT_AIR_NAME
,s.VOYAGE_TYPE
,s.DIRDIS
,s.ABROAD_AIRPORT_COUNTRY
,s.IS_LOCPS_AIRPORT_DEP
,s.IS_LOCPS_AIRPORT
,s.GRP_FIT_MARK
,s.VOYAGE_MARK
,s.CHN_AREA
,s.CHN_NATURE
,s.CHN_DETAIL1
,s.CHN_DETAIL2`,
		},
		{
			ID:          3,
			Name:        "hive intermediate temp file",
			Type:        "initialization",
			CommandType: "hivesql",
			Script: `SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapreduce.output.fileoutputformat.compress.type=BLOCK;
INSERT OVERWRITE DIRECTORY '/tmp/hive/hive/T_DWS_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
  date_format(current_timestamp, 'yyyyMMddHHmmss')  AS ETL_TIME,
  dt                                                AS DATA_MONTH,
  from_unixtime(unix_timestamp(SELL_DATE,'yyyyMMdd'),'yyyy-MM-dd') AS SELL_DATE,
  SELL_WEEK,
  SELL_YM,
  SELL_Y,
  VOYAGE,
  BUS_DEP,
  ISS_AIR_NAME,
  DAF_MARK,
  DIRDIS_MARK,
  ABROAD_AIRPORT_AREA,
  IS_LOCPS_AIRPORT_DEP,
  IS_LOCPS_AIRPORT,
  TEAM_MARK,
  VOYAGE_MARK,
  CHN_AREA,
  CHN_NATURE,
  CHN_DETAIL_1,
  CHN_DETAIL_2,
  SALE_AMT,
  SALE_NUM
FROM DWS.T_DWS_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR`,
		},
		{
			ID:          4,
			Name:        "hive intermediate temp file",
			Type:        "incremental",
			CommandType: "hivesql",
			Script: `SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapreduce.output.fileoutputformat.compress.type=BLOCK;
INSERT OVERWRITE DIRECTORY '/tmp/hive/hive/T_DWS_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
  date_format(current_timestamp, 'yyyyMMddHHmmss')  AS ETL_TIME,
  dt                                                AS DATA_MONTH,
  from_unixtime(unix_timestamp(SELL_DATE,'yyyyMMdd'),'yyyy-MM-dd') AS SELL_DATE,
  SELL_WEEK,
  SELL_YM,
  SELL_Y,
  VOYAGE,
  BUS_DEP,
  ISS_AIR_NAME,
  DAF_MARK,
  DIRDIS_MARK,
  ABROAD_AIRPORT_AREA,
  IS_LOCPS_AIRPORT_DEP,
  IS_LOCPS_AIRPORT,
  TEAM_MARK,
  VOYAGE_MARK,
  CHN_AREA,
  CHN_NATURE,
  CHN_DETAIL_1,
  CHN_DETAIL_2,
  SALE_AMT,
  SALE_NUM
FROM DWS.T_DWS_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR
WHERE dt = '${mt1}' and DATA_MONTH<='${mt1}'
    AND DATA_MONTH>=date_format(add_months(trunc(from_unixtime(unix_timestamp('${mt1}','yyyyMM'),'yyyy-MM'),'MM'),-1),'yyyyMM');`,
		},
		{
			ID:          5,
			Name:        "create dameng intermediate table",
			Type:        "initialization",
			CommandType: "dm_proc",
			Script:      `p_create_mid_app('T_APP_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR',null);`,
		},
		{
			ID:          6,
			Name:        "create dameng intermediate table",
			Type:        "incremental",
			CommandType: "dm_proc",
			Script:      `p_create_mid_app('T_APP_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR',null);`,
		},
		{
			ID:          7,
			Name:        "data load into dameng temp table",
			Type:        "initialization",
			CommandType: "shell",
			Script: `/usr/bch/3.3.0/sqoop/bin/sqoop export \
--options-file /usr/bch/3.3.0/sqoop/conf/dm8_pro.props \
--table MID_T_APP_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR \
--export-dir /tmp/hive/hive/T_DWS_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR \
--batch \
--num-mappers 8`,
		},
		{
			ID:          8,
			Name:        "data load into dameng temp table",
			Type:        "incremental",
			CommandType: "shell",
			Script: `/usr/bch/3.3.0/sqoop/bin/sqoop export \
--options-file /usr/bch/3.3.0/sqoop/conf/dm8_pro.props \
--table MID_T_APP_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR \
--export-dir /tmp/hive/hive/T_DWS_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR \
--batch \
--num-mappers 8`,
		},
		{
			ID:          9,
			Name:        "replace dameng target table",
			Type:        "initialization",
			CommandType: "dm_proc",
			Script:      `p_replace_tgttable('T_APP_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR','DF','DATA_MONTH',NULL,NULL,null);`,
		},
		{
			ID:          10,
			Name:        "replace dameng target table",
			Type:        "incremental",
			CommandType: "dm_proc",
			Script:      `p_replace_tgttable('T_APP_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR','DI','DATA_MONTH','${mt1}',1,null);`,
		},
		{
			ID:          11,
			Name:        "delete hive intermediate temp file",
			Type:        "initialization",
			CommandType: "shell",
			Script:      `hdfs dfs -rm -r -f /tmp/hive/hive/T_DWS_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR`,
		},
		{
			ID:          12,
			Name:        "delete hive intermediate temp file",
			Type:        "incremental",
			CommandType: "shell",
			Script:      `hdfs dfs -rm -r -f /tmp/hive/hive/T_DWS_INTERNAT_CHN_STRUCT_ANALYSIS_FLYR`,
		},
	},
}

// AllBatches contains all the defined ETL processes.
// For now, it only contains the one from demo.txt.
var AllBatches = ETLBatch{
	Processes: []ETLProcess{
		DemoProcess,
		// If you had another file, say demo2.txt, you would parse it
		// into another ETLProcess object and add it here.
	},
}
