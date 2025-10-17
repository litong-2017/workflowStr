package parser

import (
	"demo/model"
	"regexp"
	"strconv"
	"strings"
)

// ParseDemoFile 解析 demo.txt 文件的内容并返回一个 DemoProcess 对象。
func ParseDemoFile(content string, processName string) *model.DemoProcess {
	var steps []model.Step

	// 使用分隔符将内容分割成每个步骤的块。
	stepBlocks := strings.Split(content, "----------------------------")

	// 用于解析每个步骤标题的正则表达式，例如 "1. hive sql（初始化）："
	// 它可以同时处理全角和半角的括号。
	headerRegex := regexp.MustCompile(`^(\d+)\.\s*(.+?)\s*[（(](.+?)[)）]：`)

	for _, block := range stepBlocks {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}

		// 每个块都有一个标题行和随后的内容。
		lines := strings.SplitN(block, "\n", 2)
		if len(lines) == 0 {
			continue
		}
		header := lines[0]

		var stepContent string
		if len(lines) > 1 {
			stepContent = strings.TrimSpace(lines[1])
		}

		matches := headerRegex.FindStringSubmatch(header)
		if len(matches) != 4 {
			// 如果块不符合预期的步骤格式，则跳过。
			continue
		}

		id, err := strconv.Atoi(matches[1])
		if err != nil {
			// 如果ID不是有效的数字，则跳过。
			continue
		}

		step := model.Step{
			ID:      id,
			Name:    strings.TrimSpace(matches[2]),
			Load:    model.LoadType(strings.TrimSpace(matches[3])),
			Content: stepContent,
		}

		steps = append(steps, step)
	}

	process := &model.DemoProcess{
		Name:  processName,
		Steps: steps,
	}

	return process
}
