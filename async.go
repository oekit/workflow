package workflow

import (
	"context"
	"fmt"
	"sync"

	"github.com/oekit/concurrency/async"
)

// AsyncFlow 异步执行多个任务。
// 返回 Future，不阻塞调用方。
// 支持 context 取消传播和任务取消。
type AsyncFlow struct {
	runnables   []Runnable
	maxProcess  int
	hookManager *HookManager
}

// NewAsync 创建一个异步流程。
// maxProcs 为最大并发数，<=0 表示不限制。
func NewAsync(maxProcs int) *AsyncFlow {
	if maxProcs <= 0 {
		maxProcs = 0
	}
	return &AsyncFlow{
		maxProcess:  maxProcs,
		hookManager: NewHookManager(),
	}
}

// Add 添加一个或多个异步任务。
// 若任务本身是子流程,当前 HookManager 会同步传播给它(与 PipeFlow.Add
// 对称,2026-09-06 复审修复)。
func (a *AsyncFlow) Add(tasks ...Runnable) *AsyncFlow {
	a.runnables = append(a.runnables, tasks...)
	for _, r := range tasks {
		setHookManagerByRunnable(r, a.hookManager)
	}
	return a
}

// Hooks 获取 Hook 管理器。
func (a *AsyncFlow) Hooks() *HookManager {
	return a.hookManager
}

// SetHookManager 设置 HookManager（用于嵌套传播）。
func (a *AsyncFlow) SetHookManager(hm *HookManager) {
	a.hookManager = hm
	for _, r := range a.runnables {
		setHookManagerByRunnable(r, hm)
	}
}

// Run 并发执行所有任务，阻塞直到全部完成或 context 取消。
// 如需非阻塞模式，请使用 RunAsync。
func (a *AsyncFlow) Run(ctx context.Context) error {
	if len(a.runnables) == 0 {
		return nil
	}

	// 用父 context 等待 Future，避免子 context 被 cancel 后 Wait 返回 context.Canceled
	// 而非实际的任务错误。
	parentCtx := ctx
	childCtx, cancel := context.WithCancel(ctx)
	future := newFuture(cancel)

	// resultFn 捕获第一个任务错误，写入 future channel。
	// 豁免规则（与 ParallelFlow 一致）：仅当流程级 context（childCtx）确实被
	// 取消/超时时，错误链中的 context.Canceled/DeadlineExceeded 才视为取消的
	// 副作用被豁免；流程 context 健康时，那是任务自身的超时/取消失败，必须上报。
	var firstErr error
	var errOnce sync.Once
	done := goSafeExecute(childCtx, cancel, a.runnables, a.maxProcess, a.hookManager, func(_ int, err error) {
		if err == nil {
			return
		}
		if isCancellationErr(err) && childCtx.Err() != nil {
			return
		}
		errOnce.Do(func() { firstErr = err })
	})

	async.GoSafe(func() {
		<-done
		// 取消兜底（对齐 ParallelFlow）：全部任务因 ctx 取消走 skip/豁免路径
		// 时 firstErr 为 nil——直接返回 nil 是假成功，上层无从感知流程未执行
		if firstErr == nil && childCtx.Err() != nil {
			firstErr = childCtx.Err()
		}
		future.ch <- firstErr
		// done 必须在结果可读之后置位：先 Store 后 send 存在
		// IsDone()==true 但 Wait() 仍阻塞的观察窗口
		future.done.Store(true)
		// 成功路径也必须 cancel：否则子 context 泄漏在 parent 的
		// children 集合里，长期存活的 app ctx 下每次 Run 累积一个
		cancel()
	})

	return future.Wait(parentCtx)
}

// RunAsync 异步执行所有任务，返回 Future。
// 调用方可以通过 Future.Wait() 等待完成，或 Future.Cancel() 取消任务。
func (a *AsyncFlow) RunAsync(ctx context.Context) *Future {
	if len(a.runnables) == 0 {
		future := newFuture(nil)
		future.done.Store(true)
		future.ch <- nil
		return future
	}

	ctx, cancel := context.WithCancel(ctx)
	future := newFuture(cancel)

	// 豁免规则与 Run 一致：仅当流程级 context 真被取消/超时时才豁免取消类
	// 错误；流程 context 健康时任务自身的超时/取消失败必须上报。
	var firstErr error
	var errOnce sync.Once
	done := goSafeExecute(ctx, cancel, a.runnables, a.maxProcess, a.hookManager, func(_ int, err error) {
		if err == nil {
			return
		}
		if isCancellationErr(err) && ctx.Err() != nil {
			return
		}
		errOnce.Do(func() { firstErr = err })
	})

	async.GoSafe(func() {
		<-done
		// 与 Run 对齐：done 必须在结果可读之后置位（先 Store 后 send 存在
		// IsDone()==true 但 Wait() 仍阻塞的观察窗口）
		future.ch <- firstErr
		future.done.Store(true)
		// 同 Run：成功路径释放子 context（与 Future.Cancel 幂等兼容）
		cancel()
	})

	return future
}

// AsyncResult 异步执行结果
type AsyncResult struct {
	Index int
	Error error
}

// RunAsyncWithResults 异步执行所有任务，返回结果通道。
// 每个任务完成后会发送结果到通道。
func (a *AsyncFlow) RunAsyncWithResults(ctx context.Context) <-chan AsyncResult {
	if len(a.runnables) == 0 {
		ch := make(chan AsyncResult)
		close(ch)
		return ch
	}

	ctx, cancel := context.WithCancel(ctx)
	results := make(chan AsyncResult, len(a.runnables))

	done := goSafeExecute(ctx, cancel, a.runnables, a.maxProcess, a.hookManager, func(idx int, err error) {
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
						resultFn(idx, fmt.Errorf("workflow: task %d cancelled: %w", idx, ctx.Err()))
					}
					return
				}
				defer func() { <-sem }()
			}

			if err := ctx.Err(); err != nil {
				if resultFn != nil {
					resultFn(idx, fmt.Errorf("workflow: task %d cancelled: %w", idx, err))
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
