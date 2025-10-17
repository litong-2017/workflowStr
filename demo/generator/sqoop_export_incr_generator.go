package generator

import (
	"fmt"
	"strings"
)

// SqoopExportIncrCommand 代表一个用于增量加载的 Sqoop export shell 命令。
// 这是一个独立的实现，以避免与之前的步骤代码复用。
type SqoopExportIncrCommand struct {
	// Sqoop 可执行文件的完整路径。
	SqoopPath string
	// 要执行的 Sqoop 命令，例如 "export"。
	Command string
	// 用于存放有键值对的参数，例如 "--table MY_TABLE"。
	Arguments map[string]string
	// 用于存放只有标志没有值的参数，例如 "--batch"。
	Flags []string
}

// Generate 方法从对象的属性中动态地构建 Sqoop 命令字符串。
func (cmd *SqoopExportIncrCommand) Generate() string {
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
	commandHeader := fmt.Sprintf("%s %s", cmd.SqoopPath, cmd.Command)
	fullCommand := fmt.Sprintf("%s \\\n%s", commandHeader, strings.Join(allArgs, " \\\n"))

	return fullCommand
}
