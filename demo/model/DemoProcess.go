package model

// DemoProcess 代表一个完整的处理流程，例如整个 demo.txt 的内容。
// 它包含了该流程下的所有步骤。
type DemoProcess struct {
	Name  string `json:"name"`
	Steps []Step `json:"steps"`
}
