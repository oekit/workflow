package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// errTask 返回固定错误的 Runnable。
type errTask struct {
	err error
}

func (t *errTask) Run(context.Context) error { return t.err }

// --- M6: 任务自身超时错误不得被吞 ---

// TestAsyncFlow_TaskTimeoutSurfaced 回归测试（M6）：流程 context 健康时，
// 任务错误链中的 context.DeadlineExceeded 是任务自身的超时失败，
// Run 必须返回该错误（此前被无条件吞掉 → 假成功）。
func TestAsyncFlow_TaskTimeoutSurfaced(t *testing.T) {
	taskErr := fmt.Errorf("db timeout: %w", context.DeadlineExceeded)
	a := NewAsync(2)
	a.Add(&errTask{err: taskErr})

	err := a.Run(context.Background())
	if err == nil {
		t.Fatal("expected task's own timeout error to be surfaced, got nil (吞错回归)")
	}
	if err.Error() != taskErr.Error() {
		t.Errorf("expected %q, got %q", taskErr.Error(), err.Error())
	}
}

// TestAsyncFlow_RunAsync_TaskTimeoutSurfaced 与 Run 相同语义，走 RunAsync/Future 路径。
func TestAsyncFlow_RunAsync_TaskTimeoutSurfaced(t *testing.T) {
	taskErr := fmt.Errorf("db timeout: %w", context.DeadlineExceeded)
	a := NewAsync(2)
	a.Add(&errTask{err: taskErr})

	future := a.RunAsync(context.Background())
	// Future.Wait 可重入（#43），这里直接走公开 API 等待最终结果
	err := future.Wait(context.Background())
	if err == nil {
		t.Fatal("expected task's own timeout error, got nil")
	}
	if err.Error() != taskErr.Error() {
		t.Errorf("expected %q, got %q", taskErr.Error(), err.Error())
	}
}

// TestParallelFlow_TaskTimeoutSurfaced 与 AsyncFlow 对齐（M6 一致性）：
// 流程 context 健康时，任务自身超时错误必须上报。
func TestParallelFlow_TaskTimeoutSurfaced(t *testing.T) {
	taskErr := fmt.Errorf("db timeout: %w", context.DeadlineExceeded)
	p := NewParallel(2)
	p.Add(&errTask{err: taskErr})

	err := p.Run(context.Background())
	if err == nil {
		t.Fatal("expected task's own timeout error to be surfaced, got nil")
	}
	// ParallelFlow 会包装为 "parallel task N failed: ..."，用 errors.Is 校验错误链
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "db timeout") {
		t.Errorf("expected wrapped db timeout error, got %q", err.Error())
	}
}

// TestAsyncFlow_FlowCancelStillExcluded 验证豁免语义的另一面：流程级 context
// 真被取消/超时时，任务的取消类错误只是取消的副作用 —— Run 返回 nil 或
// context 错误，绝不误报为其它任务失败。
func TestAsyncFlow_FlowCancelStillExcluded(t *testing.T) {
	slow := &mockRunnable{name: "slow", delay: 500 * time.Millisecond}
	a := NewAsync(2)
	a.Add(slow)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := a.Run(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("expected nil or context error, got %v", err)
	}
}

// TestParallelFlow_MixedRealAndCancelError 验证多任务混合失败时，
// 流程 context 健康的前提下真实错误总能浮出（取消副作用被豁免）。
func TestParallelFlow_MixedRealAndCancelError(t *testing.T) {
	realErr := fmt.Errorf("db timeout: %w", context.DeadlineExceeded)
	p := NewParallel(4)
	p.Add(
		&mockRunnable{name: "slow", delay: 200 * time.Millisecond}, // 会被失败任务的 cancel 打断 → 取消类错误
		&errTask{err: realErr}, // 立即失败的真实错误
	)

	err := p.Run(context.Background())
	if err == nil {
		t.Fatal("expected real task error, got nil")
	}
	// ParallelFlow 会包装为 "parallel task N failed: ..."，用 errors.Is 校验错误链
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "db timeout") {
		t.Errorf("expected wrapped db timeout error, got %q", err.Error())
	}
}

// TestAsyncFlow_CancelledCtxReturnsError 回归：ctx 预先取消时所有任务走
// skip 路径且错误被豁免，firstErr 保持 nil——Run 曾返回 nil（假成功），
// 上层无从感知流程未执行。对齐 Pipe/Parallel 的语义返回取消错误。
func TestAsyncFlow_CancelledCtxReturnsError(t *testing.T) {
	ran := false
	flow := NewAsync(2)
	flow.Add(&okTask{ran: &ran})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := flow.Run(ctx)
	if err == nil {
		t.Fatal("Run with pre-cancelled ctx must return error (nil = silent fake success)")
	}
	if ran {
		t.Error("tasks must not run after cancellation")
	}
}

// TestRunAsync_ExternalCancel_NotFakeSuccess 钉住 #35：外部 cancel/超时后
// 全部任务走 skip/豁免路径 ⇒ firstErr 保持 nil ⇒ Wait 返回 nil = 假成功。
// 修复后必须返回 context.Canceled（对齐 Run 同场景）；
// Future.Cancel() 显式取消保持「取消即成功返回」语义（TestAsyncFlow_Cancel）。
func TestRunAsync_ExternalCancel_NotFakeSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// gate：两任务阻塞在 gate 上（保证外部取消发生时任务仍在执行中，
	// 走 skip/豁免路径 ⇒ firstErr 保持 nil——正是 #35 的假成功形态）
	gate := make(chan struct{})
	task := func() Runnable { return &gateRunnable{gate: gate} }
	f := NewAsync(2).Add(task(), task()).RunAsync(ctx)

	// 任务已阻塞，外部取消
	cancel()
	close(gate) // 放行 goroutine 收尾

	err := f.Wait(context.Background())
	if err == nil {
		t.Fatal("外部取消后 Wait 返回 nil = 假成功（#35）")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

// gateRunnable 阻塞在 gate 上直至其关闭或 ctx 取消（#35 用例专用）。
type gateRunnable struct{ gate chan struct{} }

func (g *gateRunnable) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.gate:
		return nil
	}
}

// --- #45: RunAsyncWithResults 套用取消类错误豁免（口径与 Run/RunAsync 对齐） ---

// collectResults 读尽结果通道并按 Index 建立映射（#45 测试用；
// 重复 Index 直接判失败，钉住「每个任务恰好一条结果」）。
func collectResults(t *testing.T, results <-chan AsyncResult) map[int]error {
	t.Helper()
	got := make(map[int]error)
	for r := range results {
		if _, dup := got[r.Index]; dup {
			t.Fatalf("duplicate result for index %d", r.Index)
		}
		got[r.Index] = r.Error
	}
	return got
}

// TestAsyncFlow_RunAsyncWithResults_TaskTimeoutSurfaced 回归测试（#45）：
// 流程 context 健康时，任务错误链中的 context.DeadlineExceeded 是任务自身
// 的超时失败，必须在结果通道原样上报（对齐 Run/RunAsync 的 M6 语义，
// 豁免判定不得把任务自身失败吞成 nil）。
func TestAsyncFlow_RunAsyncWithResults_TaskTimeoutSurfaced(t *testing.T) {
	taskErr := fmt.Errorf("db timeout: %w", context.DeadlineExceeded)
	a := NewAsync(2)
	a.Add(&errTask{err: taskErr})

	got := collectResults(t, a.RunAsyncWithResults(context.Background()))
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 result, got %d", len(got))
	}
	err := got[0]
	if err == nil {
		t.Fatal("expected task's own timeout error in results, got nil (吞错回归)")
	}
	if err.Error() != taskErr.Error() {
		t.Errorf("expected %q, got %q", taskErr.Error(), err.Error())
	}
}

// TestAsyncFlow_RunAsyncWithResults_FlowCancelExempted 回归测试（#45）：
// 流程级 context 被外部取消时，任务的取消类错误只是取消的副作用——
// 结果通道记为 Error: nil（豁免不上报）。修复前取消类错误被原样塞进
// 结果通道，与 Run/RunAsync 的豁免口径分叉。
func TestAsyncFlow_RunAsyncWithResults_FlowCancelExempted(t *testing.T) {
	gate := make(chan struct{})
	a := NewAsync(2)
	a.Add(&gateRunnable{gate: gate}, &gateRunnable{gate: gate})

	ctx, cancel := context.WithCancel(context.Background())
	results := a.RunAsyncWithResults(ctx)

	// 任务仍在执行（阻塞在 gate 上）时外部取消
	cancel()
	close(gate) // 放行收尾

	got := collectResults(t, results)
	if len(got) != 2 {
		t.Fatalf("expected exactly 2 results (one per task), got %d", len(got))
	}
	for idx, err := range got {
		if err != nil {
			t.Errorf("task %d: cancellation side-effect must be exempted (nil), got %v", idx, err)
		}
	}
}

// TestAsyncFlow_RunAsyncWithResults_PreCancelledCtxExempted 回归测试（#45）：
// context 预先取消时所有任务走 skip 路径，goSafeExecute 会为每个任务合成
// 「task N cancelled: context.Canceled」错误——同样在豁免判定覆盖内，
// 结果应为 nil 而不是合成取消错误。
func TestAsyncFlow_RunAsyncWithResults_PreCancelledCtxExempted(t *testing.T) {
	ran := false
	a := NewAsync(2)
	a.Add(&okTask{ran: &ran})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got := collectResults(t, a.RunAsyncWithResults(ctx))
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 result, got %d", len(got))
	}
	if got[0] != nil {
		t.Errorf("skipped task's synthetic cancel error must be exempted (nil), got %v", got[0])
	}
	if ran {
		t.Error("tasks must not run after cancellation")
	}
}

// TestAsyncFlow_RunAsyncWithResults_MixedRealAndCancelError 与
// TestParallelFlow_MixedRealAndCancelError 对齐（#45 一致性）：真实失败
// 打断慢任务产生取消副作用——真实错误原样上报。#82 修正豁免判据后，
// 被砍任务不再豁免记 nil（nil 只表示「确实跑完」），而是报 context.Canceled
// 类取消错误，「成功」与「未执行/被打断」可区分。
// （本用例此前把「被砍记 nil」钉为既定语义——#82 认定该语义错误：通道 API
// 无聚合面，nil=成功会把半成品/未执行任务静默记成功。）
func TestAsyncFlow_RunAsyncWithResults_MixedRealAndCancelError(t *testing.T) {
	realErr := errors.New("db down: real failure")
	// 屏障定序（不 sleep 猜时序）：slow 先启动并阻塞在 gate 上（在途形态
	// 钉死），kickoff 放行 failer → failer 真实失败 → 内部 cancel → slow
	// 经 ctx.Done 解除阻塞上报取消错误。
	slowStarted := make(chan struct{})
	slowGate := make(chan struct{})
	slow := funcRunnable(func(ctx context.Context) error {
		close(slowStarted)
		select {
		case <-ctx.Done():
			return ctx.Err() // 被内部取消打断：自报取消错误
		case <-slowGate:
			return nil
		}
	})
	kickoff := make(chan struct{})
	failer := funcRunnable(func(context.Context) error {
		<-kickoff
		return realErr
	})
	a := NewAsync(2)
	a.Add(slow, failer)

	results := a.RunAsyncWithResults(context.Background()) // 调用方 ctx 全程健康
	<-slowStarted
	close(kickoff)

	got := collectResults(t, results)
	if len(got) != 2 {
		t.Fatalf("expected exactly 2 results, got %d", len(got))
	}
	if err := got[0]; err == nil || !errors.Is(err, context.Canceled) {
		t.Errorf("task 0: 被砍任务须上报可区分的取消错误，不得记 Error: nil（#82），got %v", err)
	}
	if err := got[1]; !errors.Is(err, realErr) {
		t.Errorf("task 1: real error must be reported as-is, got %v", err)
	}
}

// --- #82: 豁免判据测调用方 ctx，被砍任务不得记成功 ---

// TestAsyncFlow_RunAsyncWithResults_SiblingFailureDoesNotSwallowTaskErrors 是
// #82 的回归钉：豁免判据必须测调用方 ctx，而不是框架派生的 childCtx——
// childCtx 会被框架自己在任一任务失败时 cancel（goSafeExecute 的
// if err != nil { cancel() }），判据退化成「此刻有没有兄弟失败过」。
// 两个断言：
//
//  1. 跑满并返回自身超时错误的任务（doc 点名的「db timeout: %w,
//     context.DeadlineExceeded」形态）不得被兄弟失败连带豁免成 nil——
//     无兄弟失败的对照组原样上报，实验组也必须上报（绝不吞错）；
//  2. 被兄弟失败砍掉的任务不得记 Error: nil——「成功」与「未执行/被打断」
//     必须可区分，报 context.Canceled 类取消错误。
func TestAsyncFlow_RunAsyncWithResults_SiblingFailureDoesNotSwallowTaskErrors(t *testing.T) {
	targetErr := fmt.Errorf("db timeout: %w", context.DeadlineExceeded)
	targetStarted := make(chan struct{})
	target := funcRunnable(func(ctx context.Context) error {
		close(targetStarted)
		<-ctx.Done() // 等兄弟失败引发的内部取消到达后返回自身超时错误（不看取消身份，只认自己的失败）
		return targetErr
	})
	sibling := funcRunnable(func(ctx context.Context) error {
		<-targetStarted // target 已在执行中才失败：砍掉的是在途任务
		return errors.New("sibling failed")
	})
	killed := &gateRunnable{gate: make(chan struct{})} // 永不放行 → 被兄弟失败砍掉

	a := NewAsync(4)
	a.Add(sibling, target, killed)

	got := collectResults(t, a.RunAsyncWithResults(context.Background()))
	if len(got) != 3 {
		t.Fatalf("expected exactly 3 results, got %d", len(got))
	}
	if got[0] == nil || !strings.Contains(got[0].Error(), "sibling failed") {
		t.Errorf("task 0: real sibling error must be reported as-is, got %v", got[0])
	}
	if !errors.Is(got[1], context.DeadlineExceeded) || !strings.Contains(got[1].Error(), "db timeout") {
		t.Errorf("task 1: task's own timeout error must survive sibling failure (绝不吞错), got %v (#82)", got[1])
	}
	if !errors.Is(got[2], context.Canceled) {
		t.Errorf("task 2: killed task must report context.Canceled (not nil=成功), got %v (#82)", got[2])
	}
}

// --- #61: 父 ctx 取消与任务真实错误并存时错误面不得掷硬币 ---

// failFastTask 立即返回固定错误，执行后发信号（#61 用例：确保真实错误
// 已被 resultFn 记录、流程 cancel 已触发后，再取消父 ctx）。
type failFastTask struct {
	ran chan struct{}
	err error
}

func (t *failFastTask) Run(context.Context) error {
	t.ran <- struct{}{}
	return t.err
}

// TestAsyncFlow_Run_ParentCancelAndRealError_SurfacesRealError 是 #61 的
// 回归钉：任务 A 快速失败于真实业务错误（resultFn 记录 firstErr → 流程
// cancel），任务 B 阻塞至父 ctx 取消后走豁免路径（合规），此时调用方父
// ctx 也被取消——finalizer 随后把真实错误写入 future，此前 Wait 的双就绪
// select 随机二选一（实测 29/30 返回 context.Canceled，把真实错误掩蔽为
// 「流程被取消」，与 ParallelFlow 同场景 30/30 返回真实错误分叉）。
// 修复后必须 30/30 返回真实错误。
func TestAsyncFlow_Run_ParentCancelAndRealError_SurfacesRealError(t *testing.T) {
	const rounds = 30
	realErr := errors.New("payment gateway 503")
	for i := 0; i < rounds; i++ {
		ran := make(chan struct{}, 1)
		gate := make(chan struct{}) // 任务 B 的阻塞点（经父 ctx 取消解除）
		a := NewAsync(2)
		a.Add(
			&failFastTask{ran: ran, err: realErr}, // A：快速真实失败 → firstErr → 流程 cancel
			&gateRunnable{gate: gate},             // B：阻塞至父 ctx 取消 → 豁免记 nil
		)

		parent, pcancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- a.Run(parent) }()

		<-ran     // A 已失败：firstErr 已记录、流程 cancel 已触发，B 仍阻塞
		pcancel() // 父 ctx 取消：B 解除阻塞走豁免，finalizer 随后写入真实错误

		select {
		case err := <-errCh:
			if !errors.Is(err, realErr) {
				t.Fatalf("round %d: real task error masked by ctx cancellation: got %v, want %v (#61)", i+1, err, realErr)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: Run did not return within budget (drain after cancel hung)", i+1)
		}
	}
}

// TestAsyncFlow_Run_CancelReturnsBeforeNonCooperativeTaskFinishes 是 #80 的
// 回归钉：契约「阻塞直到全部完成或 context 取消」——ctx 取消后 Run 只留
// 有界收尾窗口（#61 的真实错误浮出用），不得无界等待不合作任务自然返回。
// 修复前收尾 drain 是裸 <-future.done：任务 goroutine 不收尾 Run 就不返回，
// 任务挂多久调用方的关停流程挂多久（契约中「context 取消」一侧失效）。
// 定序全走屏障（started 信号 + 永不关闭的 block channel，t.Cleanup 放行
// 后台收尾），不 sleep 猜「任务已启动」。
func TestAsyncFlow_Run_CancelReturnsBeforeNonCooperativeTaskFinishes(t *testing.T) {
	block := make(chan struct{}) // 任务收尾完全由测试控制：断言前绝不放行
	started := make(chan struct{})
	a := NewAsync(2)
	a.Add(funcRunnable(func(context.Context) error {
		close(started)
		<-block // 不合作任务：不看 ctx，模拟未接 ctx 的阻塞调用
		return nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	<-started // 任务已阻塞在 block 上（此后对 ctx 取消无动于衷）
	cancel()  // 优雅关停

	select {
	case err := <-runErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second): // runCancelDrainGrace(500ms) 的大量级裕量
		t.Fatal("Run did not return within the bounded drain window：不合作任务让 Run 挂死（#80）")
	}
	t.Cleanup(func() { close(block) }) // 断言完毕后放行后台收尾
}

// TestAsyncFlow_RunAsync_ParentCancelAndRealError_SurfacesRealError 同型覆盖
// RunAsync 路径：真实错误已入 future，消费方以已取消的 ctx Wait 时不得把
// 真实错误掩蔽为 context.Canceled（Wait 双就绪优先非取消类缓存错误）。
func TestAsyncFlow_RunAsync_ParentCancelAndRealError_SurfacesRealError(t *testing.T) {
	const rounds = 30
	realErr := errors.New("payment gateway 503")
	for i := 0; i < rounds; i++ {
		ran := make(chan struct{}, 1)
		gate := make(chan struct{})
		a := NewAsync(2)
		a.Add(
			&failFastTask{ran: ran, err: realErr},
			&gateRunnable{gate: gate},
		)

		parent, pcancel := context.WithCancel(context.Background())
		future := a.RunAsync(parent)

		<-ran
		pcancel() // B 解除阻塞走豁免，finalizer 把真实错误写入 future

		// 等 finalizer 完成入账（同包直读 done 关闭广播，channel 同步不轮询）：
		// 钉住「结果已就绪 + 消费方 ctx 已取消」的双就绪形态——Wait 不得把
		// 真实错误掷硬币成 context.Canceled。
		<-future.done
		if err := future.Wait(parent); !errors.Is(err, realErr) {
			t.Fatalf("round %d: real task error masked by ctx cancellation: got %v, want %v (#61)", i+1, err, realErr)
		}
	}
}

// --- #82 补充：ErrSkipped 哨兵——三态中的「未执行」显式可辨 ---

// gatedFailTask 启动时发信号并阻塞在自家 gate 上，放行后返回固定错误
// （ErrSkipped 用例：maxProcess=1 下与兄弟任务竞争唯一槽位，先后不定但
// 两侧断言对称——恰一个真实失败、一个未执行）。
type gatedFailTask struct {
	startedOnce sync.Once
	started     chan struct{}
	gate        chan struct{}
	runs        atomic.Int32
	err         error
}

func (t *gatedFailTask) Run(ctx context.Context) error {
	t.runs.Add(1)
	t.startedOnce.Do(func() { close(t.started) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.gate:
		return t.err
	}
}

// TestAsyncFlow_RunAsyncWithResults_SkippedTaskMarkedErrSkipped 钉住 #82
// 三态中的「未执行」：maxProcess=1 下兄弟任务真实失败触发内部取消，排队
// 任务未开始即被跳过——结果必须携带 ErrSkipped 哨兵（errors.Is 可辨），
// 既不得记为 Error:nil（假成功），也不得伪装成执行失败。
// maxProcess=1 下两任务竞争唯一槽位，先到者持槽阻塞于自家 gate、后到者
// 排队；放行先后不定，但两侧断言对称，结果不受调度影响。
func TestAsyncFlow_RunAsyncWithResults_SkippedTaskMarkedErrSkipped(t *testing.T) {
	realErr := errors.New("db down: real failure")
	ta := &gatedFailTask{started: make(chan struct{}), gate: make(chan struct{}), err: realErr}
	tb := &gatedFailTask{started: make(chan struct{}), gate: make(chan struct{}), err: realErr}
	a := NewAsync(1) // 唯一槽位：先到者持槽阻塞，后到者排队
	a.Add(ta, tb)

	results := a.RunAsyncWithResults(context.Background())
	// 屏障：任一任务已持槽执行——槽位独占，另一个必然尚未开始
	select {
	case <-ta.started:
	case <-tb.started:
	}
	close(ta.gate)
	close(tb.gate)

	got := collectResults(t, results)
	if len(got) != 2 {
		t.Fatalf("expected exactly 2 results, got %d", len(got))
	}
	tasks := []*gatedFailTask{ta, tb}
	failed, skipped := 0, 0
	for idx, task := range tasks {
		err := got[idx]
		switch {
		case errors.Is(err, realErr):
			failed++
			if got := task.runs.Load(); got != 1 {
				t.Errorf("failing task %d must run exactly once, got %d", idx, got)
			}
		case errors.Is(err, ErrSkipped):
			skipped++
			if got := task.runs.Load(); got != 0 {
				t.Errorf("skipped task %d must never run, got %d runs", idx, got)
			}
			if !errors.Is(err, context.Canceled) {
				t.Errorf("skip error must also chain the ctx error, got %v", err)
			}
		default:
			t.Errorf("task %d: unexpected result %v (want realErr or ErrSkipped)", idx, err)
		}
	}
	if failed != 1 || skipped != 1 {
		t.Fatalf("want exactly 1 failed + 1 skipped, got %d failed + %d skipped", failed, skipped)
	}
}

// TestAsyncFlow_RunAsyncWithResults_SkippedExemptedWhenCallerCancelled 钉住
// ErrSkipped 与 #45 豁免语义的组合面：调用方 ctx 主动取消时，未执行任务的
// ErrSkipped 合成错误仍走豁免记 nil——三态收紧不得改变调用方主动取消的
// 既有语义（ErrSkipped 只在调用方 ctx 健康时出现）。
func TestAsyncFlow_RunAsyncWithResults_SkippedExemptedWhenCallerCancelled(t *testing.T) {
	ran := false
	a := NewAsync(1)
	a.Add(&okTask{ran: &ran})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 调用方主动取消：任务未执行

	got := collectResults(t, a.RunAsyncWithResults(ctx))
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 result, got %d", len(got))
	}
	if got[0] != nil {
		t.Errorf("caller-cancelled skip must stay exempted (nil, #45), got %v", got[0])
	}
	if ran {
		t.Error("tasks must not run after cancellation")
	}
}

// TestAsyncFlow_Run_BoundedDrainStillSurfacesRealError 钉住 #80 与 #61 的
// 组合面：收尾窗口有界化不得破坏「真实错误优先」——合规响应 ctx 的任务
// 在窗口内收尾，其真实错误仍确定性浮出（屏障定序：failFast 先失败入账、
// gate 任务经父 ctx 取消收尾，不 sleep 猜）。
func TestAsyncFlow_Run_BoundedDrainStillSurfacesRealError(t *testing.T) {
	realErr := errors.New("payment gateway 503")
	gate := make(chan struct{})
	ran := make(chan struct{}, 1)
	a := NewAsync(2)
	a.Add(
		&gateRunnable{gate: gate},             // 合规任务：阻塞至父 ctx 取消后收尾
		&failFastTask{ran: ran, err: realErr}, // 快速真实失败
	)

	parent, pcancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- a.Run(parent) }()

	<-ran     // failFast 已失败：firstErr 已记录、流程 cancel 已触发
	pcancel() // 父 ctx 取消：gate 任务经 ctx.Done 收尾，真实错误在窗口内入账

	select {
	case err := <-runDone:
		if !errors.Is(err, realErr) {
			t.Fatalf("real task error must surface within bounded drain (#61), got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run must return within the bounded drain window")
	}
}
