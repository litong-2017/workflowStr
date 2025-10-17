package model

// LoadType 定义了加载类型是“初始化”还是“增量”。
type LoadType string

const (
	InitializationLoad LoadType = "初始化"
	IncrementalLoad    LoadType = "增量"
)

// Step 代表数据处理作业中的单个步骤。
type Step struct {
	ID      int      `json:"id"`
	Name    string   `json:"name"`
	Load    LoadType `json:"load"`
	Content string   `json:"content"`
}
