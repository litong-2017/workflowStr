package model

// ProcessCollection 代表一组 DemoProcess 对象。
type ProcessCollection struct {
	Name      string        `json:"name"`
	Processes []DemoProcess `json:"processes"`
}
