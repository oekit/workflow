package workflow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockRunnable 模拟 Runnable
type mockRunnable struct {
	name    string
	err     error
	delay   time.Duration
	counter *atomic.Int32
}

func (m *mockRunnable) Run(ctx context.Context) error {
	if m.delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.delay):
		}
	}
	if m.counter != nil {
		m.counter.Add(1)
	}
	return m.err
}

// mockHook 模拟 Hook
type mockHook struct {
	beforeCount atomic.Int32
	afterCount  atomic.Int32
}

func (m *mockHook) Before(ctx context.Context, r Runnable) error {
	m.beforeCount.Add(1)
	return nil
}

func (m *mockHook) After(ctx context.Context, r Runnable, err error) {
	m.afterCount.Add(1)
}

func TestPipeFlow_Success(t *testing.T) {
	counter := &atomic.Int32{}
	pipe := NewPipe()
	pipe.Add(
		&mockRunnable{name: "step1", counter: counter},
		&mockRunnable{name: "step2", counter: counter},
		&mockRunnable{name: "step3", counter: counter},
	)

	err := pipe.Run(context.Background())
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if counter.Load() != 3 {
		t.Errorf("expected 3, got %d", counter.Load())
	}
}

func TestPipeFlow_Error(t *testing.T) {
	expectedErr := errors.New("step2 failed")
	pipe := NewPipe()
	pipe.Add(
		&mockRunnable{name: "step1"},
		&mockRunnable{name: "step2", err: expectedErr},
		&mockRunnable{name: "step3"},
	)

	err := pipe.Run(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
}

func TestPipeFlow_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	pipe := NewPipe()
	pipe.Add(&mockRunnable{name: "step1"})

	err := pipe.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestParallelFlow_Success(t *testing.T) {
	counter := &atomic.Int32{}
	parallel := NewParallel(10)
	parallel.Add(
		&mockRunnable{name: "task1", counter: counter, delay: 10 * time.Millisecond},
		&mockRunnable{name: "task2", counter: counter, delay: 10 * time.Millisecond},
		&mockRunnable{name: "task3", counter: counter, delay: 10 * time.Millisecond},
	)

	err := parallel.Run(context.Background())
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if counter.Load() != 3 {
		t.Errorf("expected 3, got %d", counter.Load())
	}
}

func TestParallelFlow_Error(t *testing.T) {
	expectedErr := errors.New("task2 failed")
	parallel := NewParallel(10)
	parallel.Add(
		&mockRunnable{name: "task1", delay: 100 * time.Millisecond},
		&mockRunnable{name: "task2", err: expectedErr},
		&mockRunnable{name: "task3", delay: 100 * time.Millisecond},
	)

	err := parallel.Run(context.Background())
	if err == nil {
		t.Error("expected error")
	}
	// 错误可能是 task2 failed 或 context cancelled
	if !errors.Is(err, expectedErr) && !errors.Is(err, context.Canceled) {
		t.Errorf("expected %v or context.Canceled, got %v", expectedErr, err)
	}
}

func TestAsyncFlow_Success(t *testing.T) {
	counter := &atomic.Int32{}
	async := NewAsync(10)
	async.Add(
		&mockRunnable{name: "task1", counter: counter, delay: 10 * time.Millisecond},
		&mockRunnable{name: "task2", counter: counter, delay: 10 * time.Millisecond},
	)

	ctx := context.Background()
	future := async.RunAsync(ctx)

	err := future.Wait(ctx)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if counter.Load() != 2 {
		t.Errorf("expected 2, got %d", counter.Load())
	}
}

func TestAsyncFlow_Cancel(t *testing.T) {
	async := NewAsync(10)
	async.Add(
		&mockRunnable{name: "task1", delay: 100 * time.Millisecond},
		&mockRunnable{name: "task2", delay: 100 * time.Millisecond},
	)

	ctx := context.Background()
	future := async.RunAsync(ctx)

	// 立即取消
	future.Cancel()

	// 等待任务完成
	err := future.Wait(context.Background())
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}

	// 取消后任务应该已完成
	if !future.IsDone() {
		t.Error("expected done")
	}
}

func TestSwitchFlow_Success(t *testing.T) {
	counter := &atomic.Int32{}
	sw := NewSwitch(func(ctx context.Context) string {
		return "branch1"
	})
	sw.Case("branch1", &mockRunnable{name: "branch1", counter: counter})
	sw.Case("branch2", &mockRunnable{name: "branch2"})

	err := sw.Run(context.Background())
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if counter.Load() != 1 {
		t.Errorf("expected 1, got %d", counter.Load())
	}
}

func TestSwitchFlow_NoBranch(t *testing.T) {
	sw := NewSwitch(func(ctx context.Context) string {
		return "nonexistent"
	})
	sw.Case("branch1", &mockRunnable{name: "branch1"})

	err := sw.Run(context.Background())
	if err == nil {
		t.Error("expected error")
	}
}

func TestHookManager_Propagation(t *testing.T) {
	hook := &mockHook{}
	hm := NewHookManager()
	hm.Add(hook)

	pipe := NewPipe()
	pipe.SetHookManager(hm)
	pipe.Add(&mockRunnable{name: "step1"}, &mockRunnable{name: "step2"})

	_ = pipe.Run(context.Background())

	if hook.beforeCount.Load() != 2 {
		t.Errorf("expected 2 before hooks, got %d", hook.beforeCount.Load())
	}
	if hook.afterCount.Load() != 2 {
		t.Errorf("expected 2 after hooks, got %d", hook.afterCount.Load())
	}
}

func TestNestedFlow(t *testing.T) {
	counter := &atomic.Int32{}

	// 创建嵌套流程：Pipe -> Parallel -> Steps
	parallel := NewParallel(10)
	parallel.Add(
		&mockRunnable{name: "p1", counter: counter},
		&mockRunnable{name: "p2", counter: counter},
	)

	pipe := NewPipe()
	pipe.Add(
		&mockRunnable{name: "s1", counter: counter},
		parallel,
		&mockRunnable{name: "s3", counter: counter},
	)

	err := pipe.Run(context.Background())
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if counter.Load() != 4 {
		t.Errorf("expected 4, got %d", counter.Load())
	}
}

func TestPipeFlow_Empty(t *testing.T) {
	pipe := NewPipe()
	err := pipe.Run(context.Background())
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestParallelFlow_Empty(t *testing.T) {
	parallel := NewParallel(10)
	err := parallel.Run(context.Background())
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestParallelFlow_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	parallel := NewParallel(10)
	parallel.Add(&mockRunnable{name: "task1"})

	err := parallel.Run(ctx)
	if err == nil {
		t.Error("expected error")
	}
}

func TestAsyncFlow_Empty(t *testing.T) {
	async := NewAsync(10)
	ctx := context.Background()
	future := async.RunAsync(ctx)

	err := future.Wait(ctx)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if !future.IsDone() {
		t.Error("expected done")
	}
}

func TestAsyncFlow_Run(t *testing.T) {
	counter := &atomic.Int32{}
	async := NewAsync(10)
	async.Add(
		&mockRunnable{name: "task1", counter: counter},
		&mockRunnable{name: "task2", counter: counter},
	)

	err := async.Run(context.Background())
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if counter.Load() != 2 {
		t.Errorf("expected 2, got %d", counter.Load())
	}
}

func TestAsyncFlow_WithContext(t *testing.T) {
	async := NewAsync(10)
	async.Add(
		&mockRunnable{name: "task1", delay: 100 * time.Millisecond},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := async.Run(ctx)
	if err == nil {
		t.Error("expected error")
	}
}

func TestSwitchFlow_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sw := NewSwitch(func(ctx context.Context) string {
		return "branch1"
	})
	sw.Case("branch1", &mockRunnable{name: "branch1"})

	err := sw.Run(ctx)
	if err == nil {
		t.Error("expected error")
	}
}

func TestSwitchFlow_NilSwitchFunc(t *testing.T) {
	sw := NewSwitch(nil)
	sw.Case("branch1", &mockRunnable{name: "branch1"})

	err := sw.Run(context.Background())
	if err == nil {
		t.Error("expected error")
	}
}

func TestFuture_Wait_Timeout(t *testing.T) {
	async := NewAsync(10)
	async.Add(&mockRunnable{name: "task1", delay: 100 * time.Millisecond})

	ctx := context.Background()
	future := async.RunAsync(ctx)

	// 使用短超时
	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()

	err := future.Wait(timeoutCtx)
	if err == nil {
		t.Error("expected error")
	}
}

// TestFuture_Wait_NilCtx 回归测试（#98）：Wait(nil) 此前在阻塞 select 里
// 对 nil 接口调 Done() 直接 panic。按簇内口径兜底为 context.Background()
// （concurrency 的 retry/loop/syncx/breaker/Handle.WaitUpTo 同款）——
// Future 是导出结果句柄，直调消费方没有上游 nil 检查。
func TestFuture_Wait_NilCtx(t *testing.T) {
	t.Run("completed future returns cached result", func(t *testing.T) {
		f := newFuture(nil)
		f.complete(nil)
		//nolint:staticcheck // #98 回归：Wait(nil) 的兜底正是被测行为
		if err := f.Wait(nil); err != nil {
			t.Fatalf("Wait(nil) on completed future: expected nil, got %v", err)
		}
	})

	t.Run("error result", func(t *testing.T) {
		expectedErr := errors.New("cached failure")
		f := newFuture(nil)
		f.complete(expectedErr)
		//nolint:staticcheck // #98 回归：Wait(nil) 的兜底正是被测行为
		if err := f.Wait(nil); !errors.Is(err, expectedErr) {
			t.Fatalf("Wait(nil): expected %v, got %v", expectedErr, err)
		}
	})

	t.Run("pending future unblocks on completion", func(t *testing.T) {
		expectedErr := errors.New("late failure")
		f := newFuture(nil)
		go func() {
			time.Sleep(10 * time.Millisecond)
			f.complete(expectedErr)
		}()
		//nolint:staticcheck // #98 回归：Wait(nil) 的兜底正是被测行为
		if err := f.Wait(nil); !errors.Is(err, expectedErr) {
			t.Fatalf("Wait(nil) on pending future: expected %v, got %v", expectedErr, err)
		}
	})
}

// TestFuture_Wait_Reentrant 回归测试（#43）：Wait 曾是一次性消费（channel
// 容量 1），首次调用取走结果后，第二次调用永久阻塞。修复后结果被缓存到
// 字段，重复 Wait 返回同一个值。
func TestFuture_Wait_Reentrant(t *testing.T) {
	t.Run("success result cached", func(t *testing.T) {
		async := NewAsync(2)
		async.Add(&mockRunnable{name: "task1"})

		future := async.RunAsync(context.Background())

		first := future.Wait(context.Background())
		if first != nil {
			t.Fatalf("first Wait: expected nil, got %v", first)
		}
		// 修复前：channel 已被首次 Wait 消费，这里永久阻塞
		second := future.Wait(context.Background())
		if second != nil {
			t.Errorf("second Wait: expected nil, got %v", second)
		}
	})

	t.Run("error result cached", func(t *testing.T) {
		expectedErr := errors.New("task failed")
		async := NewAsync(2)
		async.Add(&errTask{err: expectedErr})

		future := async.RunAsync(context.Background())

		first := future.Wait(context.Background())
		if !errors.Is(first, expectedErr) {
			t.Fatalf("first Wait: expected %v, got %v", expectedErr, first)
		}
		second := future.Wait(context.Background())
		if !errors.Is(second, expectedErr) {
			t.Errorf("second Wait: expected %v, got %v", expectedErr, second)
		}
	})
}

// TestFuture_Wait_TimeoutThenRetry 回归测试（#43）：首次 Wait 超时返回
// ctx.Err() 后，任务随后真实完成——重试 Wait 必须拿到最终结果（成功或
// 失败），而不是永久阻塞或再次超时。
func TestFuture_Wait_TimeoutThenRetry(t *testing.T) {
	t.Run("retry gets success result", func(t *testing.T) {
		async := NewAsync(2)
		async.Add(&mockRunnable{name: "slow task", delay: 50 * time.Millisecond})

		future := async.RunAsync(context.Background())

		timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		first := future.Wait(timeoutCtx)
		cancel()
		if !errors.Is(first, context.DeadlineExceeded) {
			t.Fatalf("first Wait: expected DeadlineExceeded, got %v", first)
		}

		// 修复前：结果 channel 从未被消费但也不会再有新值，这里永久阻塞
		second := future.Wait(context.Background())
		if second != nil {
			t.Fatalf("retry Wait after timeout: expected nil, got %v", second)
		}
		if !future.IsDone() {
			t.Error("expected done after retry Wait")
		}
	})

	t.Run("retry gets error result", func(t *testing.T) {
		taskErr := errors.New("slow failure")
		async := NewAsync(2)
		async.Add(&mockRunnable{name: "slow task", delay: 50 * time.Millisecond, err: taskErr})

		future := async.RunAsync(context.Background())

		timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		first := future.Wait(timeoutCtx)
		cancel()
		if !errors.Is(first, context.DeadlineExceeded) {
			t.Fatalf("first Wait: expected DeadlineExceeded, got %v", first)
		}

		second := future.Wait(context.Background())
		if !errors.Is(second, taskErr) {
			t.Fatalf("retry Wait after timeout: expected %v, got %v", taskErr, second)
		}
	})
}

// TestFuture_Wait_Concurrent 验证并发正确性（#43）：多 goroutine 同时/先后
// Wait 同一 Future，全部拿到同一个结果（-race 下稳定）。修复前结果是容量
// 1 的 channel，只有一个等待者能消费到，其余永久阻塞。
func TestFuture_Wait_Concurrent(t *testing.T) {
	// gate 控制任务完成时机：等待者全部起跑前任务不完成
	gate := make(chan struct{})
	async := NewAsync(2)
	async.Add(&gateRunnable{gate: gate})

	future := async.RunAsync(context.Background())

	const waiters = 8
	var entered sync.WaitGroup
	entered.Add(waiters)
	results := make([]error, waiters)
	var wg sync.WaitGroup
	wg.Add(waiters)
	for i := 0; i < waiters; i++ {
		i := i
		go func() {
			defer wg.Done()
			entered.Done()
			results[i] = future.Wait(context.Background())
		}()
	}
	entered.Wait() // 等待者已起跑（部分先于完成阻塞在 Wait，部分可能在完成后到达）
	close(gate)    // 放行任务 → future 完成
	wg.Wait()

	for i, err := range results {
		if err != nil {
			t.Errorf("waiter %d: expected nil, got %v", i, err)
		}
	}

	// 第一波全部返回 ⇒ 完成信号已广播；此后再 Wait 必须立即返回缓存值
	for i := 0; i < 4; i++ {
		if err := future.Wait(context.Background()); err != nil {
			t.Errorf("late Wait %d: expected nil, got %v", i, err)
		}
	}
}

// TestFuture_Wait_AfterCancel_Reentrant：显式 Cancel 的语义是「放弃等待即
// 成功返回」（#35，TestAsyncFlow_Cancel 钉住）；该结果同样被缓存，
// Cancel 后重复 Wait 不会阻塞（#43）。
func TestFuture_Wait_AfterCancel_Reentrant(t *testing.T) {
	async := NewAsync(2)
	async.Add(&mockRunnable{name: "task1", delay: 200 * time.Millisecond})

	future := async.RunAsync(context.Background())
	future.Cancel()

	for i := 0; i < 2; i++ {
		err := future.Wait(context.Background())
		if err != nil {
			t.Errorf("Wait %d after Cancel: expected nil, got %v", i, err)
		}
	}
	if !future.IsDone() {
		t.Error("expected done after Cancel + Wait")
	}
}

func TestHookManager_MultipleHooks(t *testing.T) {
	hook1 := &mockHook{}
	hook2 := &mockHook{}
	hm := NewHookManager()
	hm.Add(hook1, hook2)

	pipe := NewPipe()
	pipe.SetHookManager(hm)
	pipe.Add(&mockRunnable{name: "step1"})

	_ = pipe.Run(context.Background())

	if hook1.beforeCount.Load() != 1 {
		t.Errorf("expected 1 before hook1, got %d", hook1.beforeCount.Load())
	}
	if hook2.beforeCount.Load() != 1 {
		t.Errorf("expected 1 before hook2, got %d", hook2.beforeCount.Load())
	}
}

// rejectHook 的 Before 拒绝执行（返回 error），Before/After 各计数（#81 用）。
type rejectHook struct {
	beforeCount atomic.Int32
	afterCount  atomic.Int32
}

func (h *rejectHook) Before(context.Context, Runnable) error {
	h.beforeCount.Add(1)
	return errors.New("auth denied")
}

func (h *rejectHook) After(context.Context, Runnable, error) {
	h.afterCount.Add(1)
}

// TestHookManager_AfterOnlyForHooksWithExecutedBefore 是 #81 的回归钉：
// Before 链短路后，Before 从未执行的 Hook 不得收到 After。此前 After
// 无条件遍历全部 hook——注册序 [鉴权(拒绝), 追踪] 时追踪 Hook 收到无配对
// 的 After，计数/追踪型 Hook 计数漂移、span 关闭错位。
func TestHookManager_AfterOnlyForHooksWithExecutedBefore(t *testing.T) {
	rej := &rejectHook{}
	ok := &mockHook{}
	hm := NewHookManager()
	hm.Add(rej, ok)

	pipe := NewPipe()
	pipe.SetHookManager(hm)
	pipe.Add(&mockRunnable{name: "step1"})

	err := pipe.Run(context.Background())
	if err == nil {
		t.Fatal("expected rejection error from Before short-circuit")
	}
	if got := rej.beforeCount.Load(); got != 1 {
		t.Errorf("reject hook before=%d (want 1)", got)
	}
	if got := rej.afterCount.Load(); got != 1 {
		t.Errorf("reject hook after=%d (want 1：拒绝者自身的 After 仍配对触发)", got)
	}
	if got := ok.beforeCount.Load(); got != 0 {
		t.Errorf("ok hook before=%d (want 0：Before 未执行)", got)
	}
	if got := ok.afterCount.Load(); got != 0 {
		t.Errorf("ok hook after=%d (want 0：Before 未执行的 hook 不得收到 After，#81)", got)
	}
}

// orderHook 把 Before/After 的调用顺序记录到共享切片（#84 用；
// 仅同步执行场景使用）。
type orderHook struct {
	name  string
	order *[]string
}

func (h *orderHook) Before(context.Context, Runnable) error {
	*h.order = append(*h.order, "before:"+h.name)
	return nil
}

func (h *orderHook) After(context.Context, Runnable, error) {
	*h.order = append(*h.order, "after:"+h.name)
}

// TestHookManager_AfterReverseOrder 是 #84 的回归钉：AOP/中间件洋葱模型
// 要求 After 逆序（LIFO）——Before 正序开的 span/事务必须逆序关，否则
// 外层 hook 的收尾先于内层执行（如 TracingHook 先关 span、AuditHook 的
// 事件丢失或挂错 parent；事务型 hook 的 COMMIT 先于内层收尾）。
// Before 保持注册序正序。
func TestHookManager_AfterReverseOrder(t *testing.T) {
	var order []string
	hm := NewHookManager()
	hm.Add(&orderHook{name: "A", order: &order},
		&orderHook{name: "B", order: &order},
		&orderHook{name: "C", order: &order})

	err := safeRun(context.Background(), &okTask{}, hm)
	if err != nil {
		t.Fatalf("safeRun: %v", err)
	}
	want := []string{"before:A", "before:B", "before:C", "after:C", "after:B", "after:A"}
	if len(order) != len(want) {
		t.Fatalf("hook call order: got %v, want %v (#84：After 必须逆序)", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("hook call order: got %v, want %v (#84：After 必须逆序)", order, want)
		}
	}
}

func TestHookManager_Len(t *testing.T) {
	hm := NewHookManager()
	if hm.Len() != 0 {
		t.Errorf("expected 0, got %d", hm.Len())
	}

	hm.Add(&mockHook{})
	if hm.Len() != 1 {
		t.Errorf("expected 1, got %d", hm.Len())
	}
}

func TestSetHookManagerByRunnable(t *testing.T) {
	hm := NewHookManager()

	// 测试 Flow 类型
	pipe := NewPipe()
	setHookManagerByRunnable(pipe, hm)
	// 不应该 panic

	// 测试非 Flow 类型
	mock := &mockRunnable{name: "test"}
	setHookManagerByRunnable(mock, hm)
	// 不应该 panic
}

// TestSwitchFunc_PanicRecovered 钉住 2026-09-06 修复:SwitchFunc 自身 panic
// 转为 error 返回(与 PipeFlow "panic 被捕获"契约对齐),不再炸穿调用方。
func TestSwitchFunc_PanicRecovered(t *testing.T) {
	sw := NewSwitch(func(ctx context.Context) string {
		panic("routing exploded")
	})
	ran := false
	sw.Case("a", funcRunnable(func(ctx context.Context) error {
		ran = true
		return nil
	}))

	err := sw.Run(context.Background())
	if err == nil {
		t.Fatal("expected error from switch func panic")
	}
	if !strings.Contains(err.Error(), "switch func panic") {
		t.Errorf("error should mention switch func panic, got: %v", err)
	}
	if ran {
		t.Error("branch must not run when switch func panics")
	}
}

// TestHookPropagatedWhenAddAfterSet 钉住 2026-09-06 修复:Add 晚于
// SetHookManager 时,hook 仍要传播到子流程(doc 承诺"Hook 自动递归传播",
// 原实现只在 SetHookManager 时传播,Add 顺序颠倒即静默失效)。
func TestHookPropagatedWhenAddAfterSet(t *testing.T) {
	hm := NewHookManager()
	before, after := 0, 0
	hm.Add(&countingHook{onBefore: func() { before++ }, onAfter: func() { after++ }})

	sub := NewPipe()
	sub.Add(funcRunnable(func(ctx context.Context) error { return nil }))

	pipe := NewPipe()
	pipe.SetHookManager(hm)
	pipe.Add(sub) // 关键:Add 晚于 SetHookManager

	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 递归传播语义:hook 在每一层容器/步骤各触发一次 —
	// sub 容器(经 pipe.safeRun) + sub 内部步骤(经 sub.safeRun) = 2 次
	if before != 2 || after != 2 {
		t.Errorf("hook should fire per level via propagated manager, before=%d after=%d (want 2/2)", before, after)
	}
}

// funcRunnable 用函数构造 Runnable(本文件测试用)。
type funcRunnable func(ctx context.Context) error

func (f funcRunnable) Run(ctx context.Context) error { return f(ctx) }

// countingHook 是 hook 传播测试用计数 hook。
type countingHook struct {
	onBefore func()
	onAfter  func()
}

func (h *countingHook) Before(ctx context.Context, r Runnable) error {
	h.onBefore()
	return nil
}

func (h *countingHook) After(ctx context.Context, r Runnable, err error) {
	h.onAfter()
}
