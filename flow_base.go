package workflow

import (
	"slices"
	"sync"
)

// flowHooks 是全部 Flow 类型共享的锁 + HookManager 座（#94）。
// 切片型 Flow（PipeFlow/ParallelFlow/AsyncFlow）经 flowBase 间接内嵌；
// SwitchFlow 的成员是 map（Case 覆盖语义），只内嵌本层并保留自己的
// 传播循环。
//
// 并发契约（#46）：mu 保护 hookManager——Hooks/SetHookManager 与各执行
// 方法（Run/RunAsync/RunAsyncWithResults）并发调用不构成数据竞争，各
// 执行方法以启动瞬间的快照为准。
type flowHooks struct {
	mu          sync.Mutex
	hookManager *HookManager
}

// Hooks 获取 Hook 管理器。
// 与各执行方法并发调用是安全的：本次执行继续使用快照中的旧 manager。
func (h *flowHooks) Hooks() *HookManager {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hookManager
}

// flowBase 是切片型 Flow 的公共基座（#94）：此前 Add/Hooks/SetHookManager/
// snapshot 四个方法在 PipeFlow/ParallelFlow/AsyncFlow 中逐字复制，收敛为
// 此处一次实现、经内嵌方法集提升到各具体 Flow——公开 API 形状不变。
// SwitchFlow 不内嵌本层（避免携带无用的切片成员），只内嵌 flowHooks。
type flowBase struct {
	flowHooks
	runnables []Runnable
}

// add 注册任务并把当前 HookManager 传播给新成员，被各具体 Flow 的 Add
// 包装成链式签名（Go 方法不支持类型参数，返回具体类型的链式入口须在
// 具体类型上收口）。
// 与各执行方法并发调用是安全的：本次执行以启动瞬间的任务快照为准（#46），
// 并发新增的任务不影响本次执行（下次执行生效）。
func (b *flowBase) add(tasks ...Runnable) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.runnables = append(b.runnables, tasks...)
	for _, r := range tasks {
		setHookManagerByRunnable(r, b.hookManager)
	}
}

// SetHookManager 设置 HookManager（用于嵌套传播），并递归传播到已注册的
// 子流程——"先 SetHookManager 后 Add"与"先 Add 后 SetHookManager"两种
// 顺序下 hook 均生效（2026-09-06 复审修复的传播时序契约）。
// 与各执行方法并发调用是安全的：本次执行继续使用快照中的旧 manager。
func (b *flowBase) SetHookManager(hm *HookManager) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.hookManager = hm
	for _, r := range b.runnables {
		setHookManagerByRunnable(r, hm)
	}
}

// snapshot 返回任务集合与 HookManager 的快照（#46）。
// 各执行方法经快照执行：与并发 Add/SetHookManager 互不构成数据竞争，
// 且本次执行不受快照之后变更的影响。
func (b *flowBase) snapshot() ([]Runnable, *HookManager) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.runnables), b.hookManager
}
