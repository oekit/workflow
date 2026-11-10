package workflow

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/oekit/concurrency/async"
)

// ParallelFlow 并发执行多个任务。
// 支持并发限制和 context 取消传播。
// 当任一任务失败时，取消其他正在执行的任务。
//
// 并发契约（#46）：Add/Hooks/SetHookManager 内部加锁，与 Run 并发调用
// 不构成数据竞争。Run 以启动瞬间的任务快照执行，Run 期间新增的任务
// 不影响本次执行（下次 Run 生效）。
type ParallelFlow struct {
	mu          sync.Mutex
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
// 与 Run 并发调用是安全的：本次 Run 不感知并发新增的任务（快照语义）。
func (p *ParallelFlow) Add(tasks ...Runnable) *ParallelFlow {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.runnables = append(p.runnables, tasks...)
	for _, r := range tasks {
		setHookManagerByRunnable(r, p.hookManager)
	}
	return p
}

// Hooks 获取 Hook 管理器。
func (p *ParallelFlow) Hooks() *HookManager {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hookManager
}

// SetHookManager 设置 HookManager（用于嵌套传播）。
// 与 Run 并发调用是安全的：本次 Run 继续使用快照中的旧 manager。
func (p *ParallelFlow) SetHookManager(hm *HookManager) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.hookManager = hm
	// 递归传播到子流程
	for _, r := range p.runnables {
		setHookManagerByRunnable(r, hm)
	}
}

// Run 并发执行所有任务。
// 返回第一个非 nil error，或 context 取消错误。
//
// 错误豁免规则（与 AsyncFlow 一致）：判据是调用方传入的流程级 context
// （#82：不是框架派生、会被框架在任务失败时 cancel 的内部 context）——
// 仅当调用方 context 确实被取消/超时时，错误链中的
// context.Canceled/DeadlineExceeded 才视为取消的副作用被豁免；调用方
// context 健康时，任务自身的超时/取消失败必须上报。
func (p *ParallelFlow) Run(ctx context.Context) error {
	_, hm := p.snapshot()
	return p.runWithHooks(ctx, hm)
}

// runWithHooks 以显式 HookManager 执行本次流程（#83，见 PipeFlow 同名
// 方法注释）：嵌套执行用父流程快照的 manager，不受 SetHookManager 穿透。
func (p *ParallelFlow) runWithHooks(ctx context.Context, hm *HookManager) error {
	tasks, ownHM := p.snapshot()
	if hm == nil {
		hm = ownHM
	}
	if len(tasks) == 0 {
		return nil
	}

	parentCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, len(tasks))

	done := goSafeExecute(ctx, cancel, tasks, p.maxProcess, hm, func(idx int, err error) {
		if err == nil {
			return
		}
		if isCancellationErr(err) && parentCtx.Err() != nil {
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

// snapshot 返回任务集合与 HookManager 的快照（#46）。
// Run 经快照执行：与并发 Add/SetHookManager 互不构成数据竞争，
// 且本次执行不受快照之后变更的影响。
func (p *ParallelFlow) snapshot() ([]Runnable, *HookManager) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.runnables), p.hookManager
}
