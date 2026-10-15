package workflow

import (
	"context"
	"fmt"

	"github.com/oekit/concurrency/async"
)

// ParallelFlow 并发执行多个任务。
// 支持并发限制和 context 取消传播。
// 当任一任务失败时，取消其他正在执行的任务。
type ParallelFlow struct {
	runnables   []Runnable
	maxProcess  int
	hookManager *HookManager
}

// NewParallel 创建一个并行流程。
// maxProcs 为最大并发数，<=0 表示不限制。
func NewParallel(maxProcs int) *ParallelFlow {
	if maxProcs <= 0 {
		maxProcs = 0 // 不限制
	}
	return &ParallelFlow{
		maxProcess:  maxProcs,
		hookManager: NewHookManager(),
	}
}

// Add 添加一个或多个并发任务。
// 若任务本身是子流程,当前 HookManager 会同步传播给它(与 PipeFlow.Add
// 对称,2026-09-06 复审修复)。
func (p *ParallelFlow) Add(tasks ...Runnable) *ParallelFlow {
	p.runnables = append(p.runnables, tasks...)
	for _, r := range tasks {
		setHookManagerByRunnable(r, p.hookManager)
	}
	return p
}

// Hooks 获取 Hook 管理器。
func (p *ParallelFlow) Hooks() *HookManager {
	return p.hookManager
}

// SetHookManager 设置 HookManager（用于嵌套传播）。
func (p *ParallelFlow) SetHookManager(hm *HookManager) {
	p.hookManager = hm
	// 递归传播到子流程
	for _, r := range p.runnables {
		setHookManagerByRunnable(r, hm)
	}
}

// Run 并发执行所有任务。
// 返回第一个非 nil error，或 context 取消错误。
//
// 错误豁免规则（与 AsyncFlow 一致）：仅当流程级 context 确实被取消/超时
// 时，错误链中的 context.Canceled/DeadlineExceeded 才视为取消的副作用被
// 豁免；流程 context 健康时，任务自身的超时/取消失败必须上报。
func (p *ParallelFlow) Run(ctx context.Context) error {
	if len(p.runnables) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, len(p.runnables))

	done := goSafeExecute(ctx, cancel, p.runnables, p.maxProcess, p.hookManager, func(idx int, err error) {
		if err == nil {
			return
		}
		if isCancellationErr(err) && ctx.Err() != nil {
			return
		}
		errs <- fmt.Errorf("workflow: parallel task %d failed: %w", idx, err)
	})

	async.GoSafe(func() {
		<-done
		close(errs)
	})

	var firstErr error
	for err := range errs {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return firstErr
	}
	// 无真实任务错误但流程被取消：返回 context 取消错误（契约不变，
	// 取消副作用类错误已在上方被豁免，不能因此假成功）。
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}
