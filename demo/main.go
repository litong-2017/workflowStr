package main

import (
	"fmt"
	"log"
	"os"

	// "demo/model"
	"demo/parser"
	// "demo/steps"
)

func main() {
	// // 在实际应用中，您会从文件系统读取文件内容。
	// // 如果您在不同的位置运行程序，需要调整文件路径。
	// filePath := "demo.txt"
	// content, err := ioutil.ReadFile(filePath)
	// if err != nil {
	// 	log.Fatalf("读取文件 %s 失败: %v", filePath, err)
	// }

	// // 1. 为 demo.txt 生成一个包含所有步骤的 Go 对象。
	// // 我为这个流程提供了一个描述性的名称。
	// demoProcess := parser.ParseDemoFile(string(content), "International Channel Structure Analysis")

	// // 2. 生成一个可以容纳多个 "demo.txt" 流程对象的对象。
	// processCollection := model.ProcessCollection{
	// 	Name: "Data Warehouse ETL Jobs",
	// 	// 这里我们将第一个流程添加到集合中。
	// 	Processes: []model.DemoProcess{*demoProcess},
	// }

	// // 为了演示，我将最终的集合对象封送为 JSON 并打印出来。
	// // 这是可视化生成的 Go 对象结构并确认解析成功的好方法。
	// jsonOutput, err := json.MarshalIndent(processCollection, "", "  ")
	// if err != nil {
	// 	log.Fatalf("封送为 JSON 失败: %v", err)
	// }

	// // 这个输出可以重定向到文件或根据需要使用。
	// fmt.Println("成功解析文件并生成了 Go 对象。")
	// fmt.Println("下面是对象集合的 JSON 表示：")
	// fmt.Println(string(jsonOutput))

	// // 1. Get the pre-configured Go object for the "hive sql (initialization)" step.
	// // This object contains all the "variables" for the query.
	// hiveInitSQL := steps.GetStep1HiveInitConfig()

	// // 2. Use the object's GenerateSQL method to build the final query string.
	// // This demonstrates how the "constants" (query structure) and "variables" (object fields)
	// // are combined to create the desired output.
	// generatedSQL, err := hiveInitSQL.GenerateSQL(), error(nil)
	// if err != nil {
	// 	fmt.Printf("Error generating SQL: %v\n", err)
	// 	return
	// }

	// // 3. Print the dynamically generated SQL.
	// fmt.Println("--- Dynamically Generated Hive SQL (Initialization) ---")
	// fmt.Println(generatedSQL)
	// fmt.Println("-----------------------------------------------------")

	// // --- Step 2: 独立的增量 SQL 实现 ---
	// // 获取为步骤二专门配置的、完全独立的对象
	// hiveIncrSQL := steps.GetStep2HiveIncrementalStandaloneConfig()

	// // 使用新的独立生成器来构建 SQL 字符串
	// generatedIncrSQL := hiveIncrSQL.Generate()

	// fmt.Println("--- (独立实现) 动态生成的 Hive SQL (步骤 2: 增量) ---")
	// fmt.Println(generatedIncrSQL)
	// fmt.Println("-----------------------------------------------------------------")

	// // --- 步骤 3: 独立的 Hive 导出到 HDFS (初始化) 实现 ---
	// // 获取为步骤三专门配置的、完全独立的对象
	// hiveToHdfsScript := steps.GetStep3HiveToHdfsInitConfig()

	// // 使用新的独立生成器来构建脚本字符串
	// generatedScript := hiveToHdfsScript.Generate()

	// fmt.Println("--- (独立实现) 动态生成的 Hive 脚本 (步骤 3: 导出到HDFS) ---")
	// fmt.Println(generatedScript)
	// fmt.Println("-------------------------------------------------------------------")

	// // --- 步骤 4: Hive 导出到 HDFS (增量) - 独立实现 ---
	// hiveToHdfsIncrScript := steps.GetStep4HiveToHdfsIncrConfig()
	// generatedHdfsIncrScript := hiveToHdfsIncrScript.Generate()
	// fmt.Println("--- (独立实现) 动态生成的 Hive 脚本 (步骤 4: 增量导出到HDFS) ---")
	// fmt.Println(generatedHdfsIncrScript)
	// fmt.Println("-------------------------------------------------------------------------")

	// // --- 步骤 5: 创建达梦中间表 (初始化) - 独立实现 ---
	// dmProcCall := steps.GetStep5CreateDmMidTableInitConfig()
	// generatedProcCall := dmProcCall.Generate()
	// fmt.Println("--- (独立实现) 动态生成的存储过程调用 (步骤 5: 创建达梦中间表) ---")
	// fmt.Println(generatedProcCall)
	// fmt.Println("-----------------------------------------------------------------------")

	// // --- 步骤 6: 创建达梦中间表 (增量) - 独立实现 ---
	// dmProcCallIncr := steps.GetStep6CreateDmMidTableIncrConfig()
	// generatedProcCallIncr := dmProcCallIncr.Generate()
	// fmt.Println("--- (独立实现) 动态生成的存储过程调用 (步骤 6: 创建达梦中间表-增量) ---")
	// fmt.Println(generatedProcCallIncr)
	// fmt.Println("-----------------------------------------------------------------------------")

	// // --- 步骤 7: 数据载入达梦临时表 (初始化) - 独立实现 ---
	// sqoopExportInitCmd := steps.GetStep7SqoopExportInitConfig()
	// generatedSqoopExportInit := sqoopExportInitCmd.Generate()
	// fmt.Println("--- (独立实现) 动态生成的 Sqoop 命令 (步骤 7: 数据载入-初始化) ---")
	// fmt.Println(generatedSqoopExportInit)
	// fmt.Println("--------------------------------------------------------------------")

	// // --- 步骤 8: 数据载入达梦临时表 (增量) - 独立实现 ---
	// sqoopExportIncrCmd := steps.GetStep8SqoopExportIncrConfig()
	// generatedSqoopExportIncr := sqoopExportIncrCmd.Generate()
	// fmt.Println("--- (独立实现) 动态生成的 Sqoop 命令 (步骤 8: 数据载入-增量) ---")
	// fmt.Println(generatedSqoopExportIncr)
	// fmt.Println("------------------------------------------------------------------")
	// 1. 定义配置文件的路径
	filePath := "C:/Users/LT/Desktop/workflowStr/demo/tables.txt"
	fmt.Printf("正在读取配置文件: %s\n", filePath)

	// 2. 读取文件内容
	content, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("无法读取文件 %s: %v", filePath, err)
	}

	// 3. 调用解析器将文本转换为结构化的DwsTable对象列表
	dwsTables, err := parser.ParseTablesFile(string(content))
	if err != nil {
		log.Fatalf("解析文件内容时出错: %v", err)
	}

	fmt.Printf("成功解析出 %d 个DWS表的定义。\n", len(dwsTables))
	fmt.Println("==================================================")

	// 4. 遍历每个解析出的表定义
	for i, dwsTable := range dwsTables {
		fmt.Printf("\n--- 正在为表 [%s] 生成SQL... ---\n\n", dwsTable.Name)

		// 5. 将DwsTable对象转换为HiveSQL配置对象
		hiveSQLConfig := dwsTable.ToHiveSQLConfig()
		if hiveSQLConfig == nil {
			log.Printf("警告: 无法为表 %s 生成有效的SQL配置，已跳过。\n", dwsTable.Name)
			continue
		}

		// 6. 调用生成器，根据配置生成最终的SQL字符串
		finalSQL := hiveSQLConfig.GenerateSQL()

		// 7. 打印结果
		fmt.Println(finalSQL)
		fmt.Printf("--- 表 [%s] 的SQL已生成 (%d/%d) ---\n", dwsTable.Name, i+1, len(dwsTables))
		fmt.Println("==================================================")
	}
}
