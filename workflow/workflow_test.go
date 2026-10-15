package workflow

import (
	"context"
	"errors"
	"strings"
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
