package workflow

import (
	"context"
	"fmt"
)

// PipeFlow 顺序执行多个步骤。
// 任一步骤失败则停止并返回错误。
// 支持 context 取消传播。
//
// 并发契约（#46）：Add/Hooks/SetHookManager 内部加锁，与 Run 并发调用
// 不构成数据竞争。Run 以启动瞬间的步骤快照执行，Run 期间新增的步骤
// 不影响本次执行（下次 Run 生效）。
type PipeFlow struct {
	flowBase
}

// NewPipe 创建一个顺序流程。
func NewPipe() *PipeFlow {
	p := &PipeFlow{}
	p.hookManager = NewHookManager()
	return p
}

// Add 添加一个或多个步骤（按添加顺序执行）。
// 若步骤本身是子流程,当前 HookManager 会同步传播给它 —
// "先 SetHookManager 后 Add"与"先 Add 后 SetHookManager"两种顺序下
// hook 均生效(2026-09-06 复审修复:原实现只在 SetHookManager 时传播)。
// 与 Run 并发调用是安全的：本次 Run 不感知并发新增的步骤（快照语义）。
func (p *PipeFlow) Add(steps ...Runnable) *PipeFlow {
	p.add(steps...)
	return p
}

// Run 顺序执行所有步骤。
// 任一步骤失败或 context 取消则停止并返回错误。
// Hook 或 Runnable 的 panic 会被捕获并转为 error 返回。
func (p *PipeFlow) Run(ctx context.Context) error {
	_, hm := p.snapshot()
	return p.runWithHooks(ctx, hm)
}

// runWithHooks 以显式 HookManager 执行本次流程（#83）：嵌套执行时父流程
// 把自己快照中的 manager 传下来，本方法不读自身 manager 字段——Run 期间
// SetHookManager 的递归传播无法穿透正在进行的执行。hm 为 nil 时退回自身
// 快照（兼容外部直调）。
func (p *PipeFlow) runWithHooks(ctx context.Context, hm *HookManager) error {
	steps, ownHM := p.snapshot()
	if hm == nil {
		hm = ownHM
	}
	for i, r := range steps {
		// 检查 context 是否已取消
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("workflow: cancelled before step %d: %w", i, err)
		}

		err := safeRun(ctx, r, hm)

		if err != nil {
			return fmt.Errorf("workflow: step %d failed: %w", i, err)
		}
	}
	return nil
}
