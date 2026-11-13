package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oekit/concurrency/async"
)

// runCancelDrainGrace 是 Run 在 context 取消后等待任务收尾的宽限窗口
// （#80 引入，AsyncFlow 用；#106 起与 ParallelFlow.Run 共用同一常量与
// 语义——同仓两条流程对同一条「context 取消」契约给出同一收尾上界）。
// 窗口内收尾的任务仍能把真实错误浮出（#61 的错误面契约）；超时即返回取消
// 错误——不合作任务（不响应 ctx 的阻塞）不再让 Run 无界挂死。
const runCancelDrainGrace = 500 * time.Millisecond

// errAggregator 收敛 AsyncFlow 三个执行入口（Run/RunAsync/RunAsyncWithResults）
// 此前三写的「#82 豁免判据 + 首错聚合 + #35 取消兜底」编排（#96）：豁免
// 语义改动单点生效，各入口只保留结果出口差异（有界 drain / Future /
// 逐条 channel）。
type errAggregator struct {
	// parentCtx 是调用方传入的流程级 context（#82：判据不得用框架派生、
	// 会被框架在任务失败时 cancel 的内部 context——那会退化成「此刻有没有
	// 兄弟失败过」，把任务自身的超时/取消失败连带吞掉，doc「绝不吞错」）。
	parentCtx context.Context
	// canceled 非 nil 时指向 Future 的显式取消标记：Future.Cancel() 是
	// 调用方主动放弃等待，语义为「取消即成功返回」（#35，TestAsyncFlow_
	// Cancel 钉住），豁免与兜底都放行；无 Future 的入口为 nil。
	canceled *atomic.Bool
	once     sync.Once
	firstErr error
}

// exempted 报告任务错误是否为「调用方取消的副作用」（#45/#82）：仅当
// 调用方流程级 context 确实被取消/超时时，错误链中的
// context.Canceled/DeadlineExceeded 才豁免；Future 显式取消同样豁免。
// 调用方 context 健康时，任务自身的超时/取消失败必须原样上报。
func (ag *errAggregator) exempted(err error) bool {
	if err == nil || !isCancellationErr(err) {
		return false
	}
	return ag.parentCtx.Err() != nil || (ag.canceled != nil && ag.canceled.Load())
}

// record 是 Run/RunAsync 装配给 goSafeExecute 的 resultFn：豁免的取消
// 副作用不记为失败，其余聚合首个任务错误。
func (ag *errAggregator) record(_ int, err error) {
	if err == nil || ag.exempted(err) {
		return
	}
	ag.once.Do(func() { ag.firstErr = err })
}

// outcome 在全部任务收尾后给出 Run/RunAsync 的聚合结果（#35 取消兜底）：
// 全部任务因取消走 skip/豁免路径 ⇒ firstErr 为 nil ⇒ 直接返回 nil 是
// 假成功，上层无从感知流程未执行——补上流程 context 的取消错误。
// Future 显式取消不在此列：那是调用方主动放弃等待，语义是「取消即成功
// 返回」。
func (ag *errAggregator) outcome(ctx context.Context) error {
	if ag.firstErr != nil {
		return ag.firstErr
	}
	if err := ctx.Err(); err != nil && (ag.canceled == nil || !ag.canceled.Load()) {
		return err
	}
	return nil
}

// AsyncFlow 异步执行多个任务。
// 返回 Future，不阻塞调用方。
// 支持 context 取消传播和任务取消。
//
// 并发契约（#46）：Add/Hooks/SetHookManager 内部加锁，与 Run/RunAsync/
// RunAsyncWithResults 并发调用不构成数据竞争。各执行方法以启动瞬间的
// 任务快照执行，执行期间新增的任务不影响本次执行（下次执行生效）。
type AsyncFlow struct {
	flowBase
	maxProcess int
}

// NewAsync 创建一个异步流程。
// maxProcs 为最大并发数，<=0 表示不限制。
func NewAsync(maxProcs int) *AsyncFlow {
	a := &AsyncFlow{maxProcess: max(maxProcs, 0)}
	a.hookManager = NewHookManager()
	return a
}

// Add 添加一个或多个异步任务。
// 若任务本身是子流程,当前 HookManager 会同步传播给它(与 PipeFlow.Add
// 对称,2026-09-06 复审修复)。
// 与各执行方法并发调用是安全的：本次执行不感知并发新增的任务（快照语义）。
func (a *AsyncFlow) Add(tasks ...Runnable) *AsyncFlow {
	a.add(tasks...)
	return a
}

// Run 并发执行所有任务，阻塞直到全部完成或 context 取消。
// 如需非阻塞模式，请使用 RunAsync。
//
// 错误面契约（#61）：父 ctx 取消与任务真实错误并存时，确定性返回真实
// 任务错误（与 ParallelFlow 同场景语义对齐）；无真实错误时返回 ctx.Err()
// （#35 取消不得假成功）。
func (a *AsyncFlow) Run(ctx context.Context) error {
	_, hm := a.snapshot()
	return a.runWithHooks(ctx, hm)
}

// runWithHooks 以显式 HookManager 执行本次流程（#83，见 PipeFlow 同名
// 方法注释）：嵌套执行用父流程快照的 manager，不受 SetHookManager 穿透。
func (a *AsyncFlow) runWithHooks(ctx context.Context, hm *HookManager) error {
	tasks, ownHM := a.snapshot()
	if hm == nil {
		hm = ownHM
	}
	if len(tasks) == 0 {
		return nil
	}

	// 用父 context 等待 Future，避免子 context 被 cancel 后 Wait 返回 context.Canceled
	// 而非实际的任务错误。
	parentCtx := ctx
	childCtx, cancel := context.WithCancel(ctx)
	future := newFuture(cancel)
	// Run 的 Future 不外露、无人 Cancel，canceled 恒 false：豁免与兜底
	// 实际只由 parentCtx 判据驱动（与 RunAsync/WithResults 单源同口径）。
	ag := &errAggregator{parentCtx: parentCtx, canceled: &future.canceled}

	done := goSafeExecute(childCtx, cancel, tasks, a.maxProcess, hm, ag.record)

	async.GoSafe(func() {
		<-done
		// 取消兜底与首错聚合见 errAggregator（#35/#96）。complete 内部
		// 先写 result 再 close(done) 广播：结果可读与 IsDone==true 同时
		// 成立，且 Wait 可重入（#43）
		future.complete(ag.outcome(childCtx))
		// 成功路径也必须 cancel：否则子 context 泄漏在 parent 的
		// children 集合里，长期存活的 app ctx 下每次 Run 累积一个
		cancel()
	})

	err := future.Wait(parentCtx)
	if isCancellationErr(err) {
		// #61：父 ctx 取消与任务真实错误并存时错误面不得掷硬币——此前
		// Wait 的双就绪 select 实测 29/30 返回 context.Canceled，把真实
		// 错误（如 DB 约束冲突）掩蔽为「流程被取消」，与 ParallelFlow
		// 同场景恒返回真实错误分叉。对齐 ParallelFlow 的「先 drain 完任务
		// 结果再看 ctx」：任务与流程共享取消源（childCtx 派生自同一
		// parent），答取消的任务会很快收尾，此处等待的只是收尾窗口
		// （无真实错误时仍返回 ctx.Err()，#35 取消不得假成功）。
		//
		// #80：收尾等待必须有界——「会很快收尾」只是对合作任务成立的假设，
		// Runnable 完全可以不看 ctx（time.Sleep、未接 ctx 的 RPC 等）。裸
		// <-future.done 要等全部任务 goroutine 收尾，任务挂多久 Run 挂多久，
		// 与「阻塞直到全部完成或 context 取消」契约中「context 取消」一侧
		// 直接相悖。改为有界宽限：窗口内完成仍浮出真实错误（#61 不回退），
		// 超时即按契约返回取消错误。
		select {
		case <-future.done:
			if cached := future.cachedResult(); cached != nil && !isCancellationErr(cached) {
				return cached
			}
		case <-time.After(runCancelDrainGrace):
			return err
		}
	}
	return err
}

// RunAsync 异步执行所有任务，返回 Future。
// 调用方可以通过 Future.Wait() 等待完成，或 Future.Cancel() 取消任务。
func (a *AsyncFlow) RunAsync(ctx context.Context) *Future {
	tasks, hm := a.snapshot()
	if len(tasks) == 0 {
		future := newFuture(nil)
		future.complete(nil)
		return future
	}

	parentCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	future := newFuture(cancel)
	// 豁免判据与兜底见 errAggregator（#82/#35/#96）：Future.Cancel() 显式
	// 取消走「取消即成功返回」语义，由 canceled 标记在豁免与兜底两处生效
	// （TestAsyncFlow_Cancel 钉住）。
	ag := &errAggregator{parentCtx: parentCtx, canceled: &future.canceled}

	done := goSafeExecute(ctx, cancel, tasks, a.maxProcess, hm, ag.record)

	async.GoSafe(func() {
		<-done
		future.complete(ag.outcome(ctx))
		// 同 Run：成功路径释放子 context（与 Future.Cancel 幂等兼容）
		cancel()
	})

	return future
}

// AsyncResult 异步执行结果。
//
// Error 的三态语义（#82）——「成功 / 失败 / 未执行」可辨：
//   - Error == nil：任务确实跑完且成功（调用方 context 取消/超时时，取消
//     副作用的豁免结果同样记 nil，#45）；
//   - errors.Is(Error, ErrSkipped)：任务未执行——流程内部取消（兄弟任务
//     失败连带砍掉）后未开始即被跳过（错误链同时携带 ctx 取消错误）；
//   - 其余非 nil：任务失败——含执行中被内部取消打断的任务自报的取消错误
//     （通常 errors.Is(Error, context.Canceled)），以及任务自身的取消类
//     失败（调用方 context 健康时原样上报，绝不吞错）。
type AsyncResult struct {
	Index int
	Error error
}

// ErrSkipped 标记「任务未执行」：RunAsyncWithResults 的结果中，错误链含本
// 哨兵（errors.Is 判定）表示该任务因流程内部取消（兄弟任务失败连带）未开始
// 即被跳过——三态中的「未执行」，区别于「确实成功」（Error == nil）与
// 「执行失败」（其余非 nil）。
var ErrSkipped = errors.New("task skipped")

// skippedErr 合成「任务未执行」错误（#82）：ctx 取消导致任务未开始即被
// 跳过。错误链同时携带 context 错误与 ErrSkipped 哨兵——调用方经 errors.Is
// 判定三态中的「未执行」，与「执行失败」「确实成功」区分。
func skippedErr(idx int, ctx context.Context) error {
	return fmt.Errorf("workflow: task %d not started: %w (%w)", idx, ctx.Err(), ErrSkipped)
}

// RunAsyncWithResults 异步执行所有任务，返回结果通道。
// 每个任务完成后会发送一条 AsyncResult 到通道（被豁免的任务同样有且仅有一条
// 结果），全部任务结束后关闭通道。
//
// 错误豁免规则与 Run/RunAsync 一致（#45），判据是调用方传入的流程级
// context（#82：不是框架派生、会被框架在任务失败时 cancel 的内部
// context）：仅当调用方 context 确实被取消/超时时，错误链中的
// context.Canceled/DeadlineExceeded 才视为取消的副作用被豁免——该任务的
// 结果记为 Error: nil（不计为任务失败，每个任务恰好一条结果，Index
// 连续）；调用方 context 健康时，任务自身的超时/取消失败必须原样上报。
// 兄弟任务失败触发的框架内部取消不豁免：被砍/未执行的任务报
// context.Canceled 类错误（未开始即被跳过的任务错误链另携带 ErrSkipped
// 哨兵，见 AsyncResult 三态），与「确实跑完」（Error: nil）可区分。
// 因此「通道内全部 Error 为 nil」不代表流程未被取消，调用方需结合自身
// context 判断整体执行状态。
func (a *AsyncFlow) RunAsyncWithResults(ctx context.Context) <-chan AsyncResult {
	tasks, hm := a.snapshot()
	if len(tasks) == 0 {
		ch := make(chan AsyncResult)
		close(ch)
		return ch
	}

	// 豁免判定与 Run/RunAsync 单源（errAggregator，#96）：无 Future 即无
	// 显式取消通道，canceled 为 nil——Future.Cancel 豁免不适用本入口。
	ag := &errAggregator{parentCtx: ctx}
	ctx, cancel := context.WithCancel(ctx)
	results := make(chan AsyncResult, len(tasks))

	done := goSafeExecute(ctx, cancel, tasks, a.maxProcess, hm, func(idx int, err error) {
		// 被豁免（调用方取消的副作用）的任务结果记 nil（不计为任务失败，
		// 每个任务恰好一条结果，Index 连续）；兄弟任务失败触发的框架内部
		// cancel 不豁免（判据测 parentCtx）：被砍/未执行的任务报
		// context.Canceled 类错误，不得记 nil=成功。
		if ag.exempted(err) {
			err = nil
		}
		results <- AsyncResult{Index: idx, Error: err}
	})

	async.GoSafe(func() {
		<-done
		cancel() // 确保 cancel 被调用
		close(results)
	})

	return results
}

// goSafeExecute 并发执行一组 Runnable，抽取自 ParallelFlow 和 AsyncFlow 的公共编排逻辑。
//
// resultFn 在每个任务完成后调用（err 可能为 nil）。
// 如果 resultFn 不为 nil 且任务因 context 取消而跳过，会调用 resultFn 报告取消错误。
// 如果 resultFn 为 nil（如 Run/RunAsync），context 取消时静默跳过。
//
// 返回一个 channel，当所有 goroutine（含 semaphore 等待中的）完成时关闭。
func goSafeExecute(
	ctx context.Context,
	cancel context.CancelFunc,
	runnables []Runnable,
	maxProcess int,
	hookManager *HookManager,
	resultFn func(idx int, err error),
) <-chan struct{} {
	done := make(chan struct{})

	var sem chan struct{}
	if maxProcess > 0 {
		sem = make(chan struct{}, maxProcess)
	}

	var wg sync.WaitGroup
	for i, r := range runnables {
		wg.Add(1)
		idx := i
		runnable := r
		async.GoSafe(func() {
			defer wg.Done()

			if sem != nil {
				// 感知取消：否则取消后排队的任务仍要逐个穿过信号量空转
				// 才退出（maxProcess 小 + 任务多时 drain 拖长停机）
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					if resultFn != nil {
						resultFn(idx, skippedErr(idx, ctx))
					}
					return
				}
				defer func() { <-sem }()
			}

			if err := ctx.Err(); err != nil {
				if resultFn != nil {
					resultFn(idx, skippedErr(idx, ctx))
				}
				return
			}

			// 使用 safeRun 捕获 hook/Runnable 的 panic 并转为 error
			err := safeRun(ctx, runnable, hookManager)

			if resultFn != nil {
				resultFn(idx, err)
			}

			if err != nil {
				cancel()
			}
		})
	}

	async.GoSafe(func() {
		wg.Wait()
		close(done)
	})

	return done
}
