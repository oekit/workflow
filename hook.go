package workflow

import "context"

// Hook 生命周期钩子接口。
// 用于在 Runnable 执行前后插入通用逻辑（如日志、监控、链路追踪、权限校验）。
type Hook interface {
	// Before 在 Runnable 执行前调用。
	// 返回 error 时中止执行（HookManager.Before 短路传播）。
	Before(ctx context.Context, r Runnable) error

	// After 在 Runnable 执行后调用（包括 Run panic 时通过 defer 触发）。
	// 与 Before 配对（#81）：Before 未执行的 Hook 不会收到 After；Before
	// 短路拒绝的 Hook 自身仍会收到一次 After。
	// err 为 nil 表示执行成功，非 nil 表示执行失败。
	After(ctx context.Context, r Runnable, err error)
}
