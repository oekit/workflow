package workflow

import "context"

// Runnable 可执行单元的基础接口。
// 所有流程节点（叶子节点和容器 Flow）都必须实现此接口。
type Runnable interface {
	// Run 执行业务逻辑。
	// 返回 error 表示执行失败，会触发流程中断或取消。
	Run(ctx context.Context) error
}
