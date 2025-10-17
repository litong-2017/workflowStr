package generator

import (
	"fmt"
	"strings"
)

// SqoopExportCommand 代表一个通用的 Sqoop export shell 命令。
// 它的设计目的是为了可以灵活配置各种 Sqoop 导出任务。
type SqoopExportCommand struct {
	// Sqoop 可执行文件的完整路径。
	SqoopPath string
	// 要执行的 Sqoop 命令，例如 "export"。
	Command string
	// 用于存放有键值对的参数，例如 "--table MY_TABLE"。
	// 键: "table", 值: "MY_TABLE"。
	Arguments map[string]string
	// 用于存放只有标志没有值的参数，例如 "--batch"。
	Flags []string
}

// Generate 方法从对象的属性中动态地构建 Sqoop 命令字符串。
func (cmd *SqoopExportCommand) Generate() string {
	var allArgs []string

	// 将带值的参数添加到列表中
	for key, value := range cmd.Arguments {
		allArgs = append(allArgs, fmt.Sprintf("--%s %s", key, value))
	}

	// 将标志参数添加到列表中
	for _, flag := range cmd.Flags {
		allArgs = append(allArgs, fmt.Sprintf("--%s", flag))
	}

	// 构建最终的命令字符串
	// 首先是可执行文件和命令
	commandHeader := fmt.Sprintf("%s %s", cmd.SqoopPath, cmd.Command)

	// 使用 " \" 将所有参数连接起来，以实现多行格式化
	// 这会生成一个清晰、可读的 shell 命令。
	fullCommand := fmt.Sprintf("%s \\\n%s", commandHeader, strings.Join(allArgs, " \\\n"))

	return fullCommand
}
