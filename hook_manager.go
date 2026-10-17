package workflow

import "context"

// HookManager 钩子管理器。
// 按注册顺序依次执行所有 Hook，支持递归传播到子流程。
//
// 并发契约：Add 非 goroutine 安全——所有 Hook 必须在 Run 之前注册完毕
// （Run 期间 Add 是数据竞争）。Run 期间同一 Hook 会被并发调用
// （Parallel/Async 多 goroutine），Hook 实现必须并发安全且自担幂等；
// 嵌套流程会逐层各触发一次（非端到端一次）。
type HookManager struct {
	hooks []Hook
}

// NewHookManager 创建一个新的 HookManager。
func NewHookManager() *HookManager {
	return &HookManager{}
}

// Add 添加一个或多个 Hook。
func (m *HookManager) Add(hooks ...Hook) {
	m.hooks = append(m.hooks, hooks...)
}

// Before 按注册顺序依次调用所有 Hook 的 Before 方法。
// 遇到第一个返回 error 的 Hook 时短路返回该 error。
func (m *HookManager) Before(ctx context.Context, r Runnable) error {
	for _, h := range m.hooks {
		if err := h.Before(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// After 按注册顺序依次调用所有 Hook 的 After 方法。
func (m *HookManager) After(ctx context.Context, r Runnable, err error) {
	for _, h := range m.hooks {
		h.After(ctx, r, err)
	}
}

// Len 返回 Hook 数量。
func (m *HookManager) Len() int {
	return len(m.hooks)
}
