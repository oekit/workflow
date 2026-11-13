package workflow

import (
	"context"
	"fmt"
	"time"

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
	flowBase
	maxProcess int
}

// NewParallel 创建一个并行流程。
// maxProcs 为最大并发数，<=0 表示不限制。
func NewParallel(maxProcs int) *ParallelFlow {
	p := &ParallelFlow{maxProcess: max(maxProcs, 0)}
	p.hookManager = NewHookManager()
	return p
}

// Add 添加一个或多个并发任务。
// 若任务本身是子流程,当前 HookManager 会同步传播给它(与 PipeFlow.Add
// 对称,2026-09-06 复审修复)。
// 与 Run 并发调用是安全的：本次 Run 不感知并发新增的任务（快照语义）。
func (p *ParallelFlow) Add(tasks ...Runnable) *ParallelFlow {
	p.add(tasks...)
	return p
}

// Run 并发执行所有任务，阻塞直到全部完成或 context 取消。
// 返回第一个非 nil error，或 context 取消错误。
//
// 取消后的收尾等待有界（#106，与 AsyncFlow 的 #80 修法同口径）：调用方
// context 取消/超时后，等待任务收尾最多再留 runCancelDrainGrace（500ms）
// 的宽限窗口浮出真实错误，超时即按契约返回取消错误——不合作任务（不响应
// ctx 的阻塞调用）不再让 Run 无界挂死。正常路径（context 健康的长任务）
// 仍阻塞到全部任务完成，不受宽限窗口约束。
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
	ctx = orBackground(ctx)
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
	firstErrCh := make(chan error, 1)

	done := goSafeExecute(ctx, cancel, tasks, p.maxProcess, hm, func(idx int, err error) {
		if err == nil {
			return
		}
		if isCancellationErr(err) && parentCtx.Err() != nil {
			return
		}
		errs <- fmt.Errorf("workflow: parallel task %d failed: %w", idx, err)
	})

	// errs 的收口与首错聚合移到独立 goroutine（#106）：errs 容量 = 任务数，
	// 发送永不阻塞（resultFn 完成先于 done 关闭），本 goroutine 至多存活到
	// 任务全部收尾——不合作任务仍在跑时它随任务一起滞留，不额外泄漏。
	// 此前的 `for err := range errs` 在 Run 的调用 goroutine 上裸等 errs 关闭
	// （即裸等 done），ctx 取消后要等**全部**任务收尾，不合作任务挂多久
	// Run 就挂多久——「context 取消」一侧契约失效，与 #80 定性的缺陷族同款。
	async.GoSafe(func() {
		<-done // 全部任务 goroutine 收尾：此后 errs 不再有新发送，可安全收口
		close(errs)
		var firstErr error
		for err := range errs {
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
		firstErrCh <- firstErr
	})

	// 正常路径：阻塞到全部任务收尾（errs 关闭 ⇒ 首错聚合完成），不受
	// 宽限窗口约束；调用方 context 取消后收尾等待有界（#106，与 AsyncFlow
	// 的 #80 修法同款）——窗口内收尾仍浮出真实错误（#61 语义不回退），
	// 超时即按契约返回取消错误。
	var firstErr error
	select {
	case firstErr = <-firstErrCh:
	case <-parentCtx.Done():
		select {
		case firstErr = <-firstErrCh:
		case <-time.After(runCancelDrainGrace):
			return ctx.Err()
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
