package workflow

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
)

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
// 单个 hook 的 Before panic 转为 error 返回，不再炸穿调用方。
func (m *HookManager) Before(ctx context.Context, r Runnable) error {
	_, err := m.beforeRun(ctx, r)
	return err
}

// After 按逆注册序依次调用所有 Hook 的 After 方法（#84：AOP 洋葱配对，
// Before 正序开、After 逆序关）。
// 这是无配对信息的直调入口：对全部 hook 触发。safeRun 执行路径不走本
// 方法——其 After 与 Before 逐 hook 配对触发（见 afterRun，#81）。
// 单个 hook 的 After panic 被逐 hook recover（聚合为 error，执行路径由
// safeRun 上报；本入口丢弃）。
func (m *HookManager) After(ctx context.Context, r Runnable, err error) {
	_ = m.afterRun(ctx, r, err, len(m.hooks))
}

// beforeRun 按注册顺序依次调用所有 Hook 的 Before，遇首个错误短路。
// 返回 n = 已执行 Before 的 hook 数（含短路拒绝者自身；其 Before panic
// 同样计入并转为 error）——After 只对这 n 个 hook 配对触发（#81），
// Before 未执行的 hook 不得收到 After（计数/追踪型 Hook 防计数漂移、
// span 关闭错位）。
func (m *HookManager) beforeRun(ctx context.Context, r Runnable) (int, error) {
	for i, h := range m.hooks {
		berr := callHookBefore(ctx, h, r)
		if berr != nil {
			return i + 1, berr
		}
	}
	return len(m.hooks), nil
}

// afterRun 对前 n 个 hook（其 Before 已执行）按逆注册序触发 After（#81
// 配对；#84 逆序 = AOP 洋葱配对——Before 正序开的 span/事务逆序关，
// span/事务类嵌套 hook 不再交叉关闭），逐 hook recover：
// 任一 hook 的 After panic 不得截断后续 hook 的 After
// （配对进一步撕裂），panic 聚合为 error 返回（由 safeRun 按 #62 口径
// 与任务错误 join）。
func (m *HookManager) afterRun(ctx context.Context, r Runnable, err error, n int) error {
	var hookErr error
	for i := n - 1; i >= 0; i-- {
		hookErr = errors.Join(hookErr, callHookAfter(ctx, m.hooks[i], r, err))
	}
	return hookErr
}

// callHookBefore 调用单个 hook 的 Before 并把 panic 转为 error（#81：
// 配对计数不受 panic 影响——panic 的 hook 其 Before 已执行，仍配对 After）。
func callHookBefore(ctx context.Context, h Hook, r Runnable) (err error) {
	defer func() {
		if rp := recover(); rp != nil {
			err = fmt.Errorf("workflow: before hook panic: %v\n%s", rp, debug.Stack())
		}
	}()
	return h.Before(ctx, r)
}

// callHookAfter 调用单个 hook 的 After 并把 panic 转为 error（#81）。
func callHookAfter(ctx context.Context, h Hook, r Runnable, err error) (aerr error) {
	defer func() {
		if rp := recover(); rp != nil {
			aerr = fmt.Errorf("workflow: after hook panic: %v\n%s", rp, debug.Stack())
		}
	}()
	h.After(ctx, r, err)
	return nil
}

// Len 返回 Hook 数量。
func (m *HookManager) Len() int {
	return len(m.hooks)
}
