package workflow

import (
	"context"
	"fmt"
)

// PipeFlow 顺序执行多个步骤。
// 任一步骤失败则停止并返回错误。
// 支持 context 取消传播。
type PipeFlow struct {
	runnables   []Runnable
	hookManager *HookManager
}

// NewPipe 创建一个顺序流程。
func NewPipe() *PipeFlow {
	return &PipeFlow{
		hookManager: NewHookManager(),
	}
}

// Add 添加一个或多个步骤（按添加顺序执行）。
// 若步骤本身是子流程,当前 HookManager 会同步传播给它 —
// "先 SetHookManager 后 Add"与"先 Add 后 SetHookManager"两种顺序下
// hook 均生效(2026-09-06 复审修复:原实现只在 SetHookManager 时传播)。
func (p *PipeFlow) Add(steps ...Runnable) *PipeFlow {
	p.runnables = append(p.runnables, steps...)
	for _, r := range steps {
		setHookManagerByRunnable(r, p.hookManager)
	}
	return p
}

// Hooks 获取 Hook 管理器。
func (p *PipeFlow) Hooks() *HookManager {
	return p.hookManager
}

// SetHookManager 设置 HookManager（用于嵌套传播）。
func (p *PipeFlow) SetHookManager(hm *HookManager) {
	p.hookManager = hm
	// 递归传播到子流程
	for _, r := range p.runnables {
		setHookManagerByRunnable(r, hm)
	}
}

// Run 顺序执行所有步骤。
// 任一步骤失败或 context 取消则停止并返回错误。
// Hook 或 Runnable 的 panic 会被捕获并转为 error 返回。
func (p *PipeFlow) Run(ctx context.Context) error {
	for i, r := range p.runnables {
		// 检查 context 是否已取消
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("workflow: cancelled before step %d: %w", i, err)
		}

		err := safeRun(ctx, r, p.hookManager)

		if err != nil {
			return fmt.Errorf("workflow: step %d failed: %w", i, err)
		}
	}
	return nil
}
