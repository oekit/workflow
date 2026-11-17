package workflow

import (
	"context"
	"time"

	pkgerrors "github.com/oekit/pkg/errors"
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
// 错误豁免规则（与 AsyncFlow 单源 errAggregator，#149）：判据是调用方
// 传入的流程级 context（#82：不是框架派生、会被框架在任务失败时 cancel
// 的内部 context）——仅当调用方 context 确实被取消/超时时，错误链中的
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

	// 豁免判据与首错聚合单源 errAggregator（#149，#96 的收敛推广到
	// ParallelFlow）：此前本入口手写「2 条件豁免 + errs channel 首错聚合」，
	// 与 errAggregator 的 3 条件口径分叉——改判据只会改一边。收敛后判据
	// 改动单点生效。ParallelFlow 无 Future，canceled 恒 nil——Future 显式
	// 取消豁免分支对本入口不可达，运行时豁免行为与手写版一致（回归钉）。
	ag := &errAggregator{parentCtx: parentCtx}

	// resultFn 出口差异仅剩包装（#149）：ParallelFlow 的错误面标注任务
	// 序号。Wrapf 对 nil 恒返回 nil；豁免判定在 record 内经 errors.Is
	// 穿透包装链，与旧版「先豁免后包装」的判定等价。
	done := goSafeExecute(ctx, cancel, tasks, p.maxProcess, hm, func(idx int, err error) {
		ag.record(idx, pkgerrors.Wrapf(err, "workflow: parallel task %d failed", idx))
	})

	// 正常路径：阻塞到全部任务收尾，不受宽限窗口约束；调用方 context 取消
	// 后收尾等待有界（#106，与 AsyncFlow 的 #80 修法同款）——窗口内收尾
	// 仍浮出真实错误（#61 语义不回退），超时即按契约返回取消错误。
	// 首错已在 record 内同步入账（once.Do）：done 在 wg.Wait() 之后关闭，
	// <-done 返回即聚合终局——旧版 errs channel + 收口 goroutine（#106
	// 引入的独立聚合 goroutine）随 #149 一并删除，Run 调用 goroutine 直接
	// 等待 done 并以宽限窗口兜底，不合作任务不再让 Run 无界挂死。
	select {
	case <-done:
	case <-parentCtx.Done():
		select {
		case <-done:
		case <-time.After(runCancelDrainGrace):
			return ctx.Err()
		}
	}
	// 首错聚合与 #35 取消兜底见 errAggregator（#149 单源）：无真实任务
	// 错误但流程被取消时返回 context 取消错误——取消副作用类错误已被
	// 豁免，不得因此假成功。
	return ag.outcome(ctx)
}
