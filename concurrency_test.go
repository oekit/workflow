package workflow

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// startedGateTask 首次执行时关闭 started 通知「Run 已持有快照并开始执行」，
// 随后阻塞在 gate 上直至放行或 ctx 取消；每次执行计数。
// #46 用例专用：把并发 Add/Case/SetHookManager 精确钉在
// 「快照已取、Run 未结束」的窗口内（不靠 sleep 凑重叠）。
type startedGateTask struct {
	startedOnce sync.Once
	started     chan struct{}
	gate        chan struct{}
	runs        atomic.Int32
}

func newStartedGateTask() *startedGateTask {
	return &startedGateTask{
		started: make(chan struct{}),
		gate:    make(chan struct{}),
	}
}

func (t *startedGateTask) Run(ctx context.Context) error {
	t.runs.Add(1)
	t.startedOnce.Do(func() { close(t.started) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.gate:
		return nil
	}
}

// TestPipeFlow_ConcurrentAddDuringRun 回归测试（#46）：Run 期间并发 Add 与
// SetHookManager 不再是数据竞争（修复前 Add 的 append 与 Run 遍历同一
// slice，-race 直接告警）。语义：Run 以启动瞬间快照执行，期间新增的步骤
// 与替换的 HookManager 不影响本次执行、下次 Run 生效。
func TestPipeFlow_ConcurrentAddDuringRun(t *testing.T) {
	// hm1 在 Run 前挂上；Run 期间被替换——本次执行必须仍用快照 hm1
	hm1 := NewHookManager()
	hook1 := &mockHook{}
	hm1.Add(hook1)

	gateTask := newStartedGateTask()
	added := &atomic.Int32{}
	pipe := NewPipe()
	pipe.SetHookManager(hm1)
	pipe.Add(gateTask)

	runDone := make(chan error, 1)
	go func() { runDone <- pipe.Run(context.Background()) }()
	<-gateTask.started // 快照已取（首步已在执行）且本次 Run 未结束

	// Run 执行期间并发 Add + 替换 HookManager
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			pipe.Add(&mockRunnable{name: fmt.Sprintf("added-%d", i), counter: added})
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		pipe.SetHookManager(NewHookManager())
	}()
	wg.Wait()
	close(gateTask.gate)

	if err := <-runDone; err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 快照语义：本次 Run 只执行快照内的任务
	if got := gateTask.runs.Load(); got != 1 {
		t.Errorf("first Run should execute only the snapshot task, got %d runs", got)
	}
	// 本次执行使用快照 hm1：hook 对首步前后各触发一次
	if got := hook1.beforeCount.Load(); got != 1 {
		t.Errorf("in-flight run must use snapshot hook manager, hook1 before=%d (want 1)", got)
	}
	if got := hook1.afterCount.Load(); got != 1 {
		t.Errorf("in-flight run must use snapshot hook manager, hook1 after=%d (want 1)", got)
	}

	// 下次 Run 生效：拾取并发新增的 8 个任务，且被替换的（空）manager 接管
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if got := gateTask.runs.Load(); got != 2 {
		t.Errorf("second Run should re-execute gate task, got %d runs", got)
	}
	if got := added.Load(); got != 8 {
		t.Errorf("second Run should pick up 8 concurrently added steps, got %d", got)
	}
	if got := hook1.beforeCount.Load(); got != 1 {
		t.Errorf("second Run must use replaced hook manager, hook1 before=%d (want 1)", got)
	}
}

// TestParallelFlow_ConcurrentAddDuringRun 回归测试（#46）：Run 期间并发
// Add 无数据竞争，且本次 Run 只执行快照内任务，下次 Run 拾取新增任务。
func TestParallelFlow_ConcurrentAddDuringRun(t *testing.T) {
	gateTask := newStartedGateTask()
	counter := &atomic.Int32{}
	p := NewParallel(2)
	p.Add(gateTask, &mockRunnable{name: "quick", counter: counter})

	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(context.Background()) }()
	<-gateTask.started

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Add(&mockRunnable{name: "added", counter: counter})
		}()
	}
	wg.Wait()
	close(gateTask.gate)

	if err := <-runDone; err != nil {
		t.Fatalf("Run: %v", err)
	}
	// quick 计 1 次（gateTask 用自身 runs 计数）；并发 Add 的任务不在本次快照内
	if got := counter.Load(); got != 1 {
		t.Errorf("first Run should execute only snapshot tasks (quick=1), got %d", got)
	}
	if got := gateTask.runs.Load(); got != 1 {
		t.Errorf("first Run should execute gate task once, got %d runs", got)
	}

	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	// 第二次快照 = 2 个初始 + 8 个新增：quick 再计 1 + 新增 8 → 总计 1+1+8 = 10
	if got := counter.Load(); got != 10 {
		t.Errorf("second Run should pick up added tasks, got %d total executions (want 10)", got)
	}
	if got := gateTask.runs.Load(); got != 2 {
		t.Errorf("gate task should run twice, got %d runs", got)
	}
}

// TestAsyncFlow_ConcurrentAddDuringRunAsync 回归测试（#46）：RunAsync 期间
// 并发 Add 无数据竞争；快照语义一致，二次执行（走 RunAsyncWithResults，
// 顺带覆盖其快照路径）拾取新增任务。
func TestAsyncFlow_ConcurrentAddDuringRunAsync(t *testing.T) {
	gateTask := newStartedGateTask()
	counter := &atomic.Int32{}
	a := NewAsync(2)
	a.Add(gateTask, &mockRunnable{name: "quick", counter: counter})

	future := a.RunAsync(context.Background())
	<-gateTask.started

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.Add(&mockRunnable{name: "added", counter: counter})
		}()
	}
	wg.Wait()
	close(gateTask.gate)

	if err := future.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	// quick 计 1 次（gateTask 用自身 runs 计数）；并发 Add 的任务不在本次快照内
	if got := counter.Load(); got != 1 {
		t.Errorf("RunAsync should execute only snapshot tasks (quick=1), got %d", got)
	}
	if got := gateTask.runs.Load(); got != 1 {
		t.Errorf("RunAsync should execute gate task once, got %d runs", got)
	}

	nResults := 0
	for r := range a.RunAsyncWithResults(context.Background()) {
		nResults++
		if r.Error != nil {
			t.Errorf("task %d: unexpected error %v", r.Index, r.Error)
		}
	}
	// 第二次快照 = 2 个初始 + 8 个新增 = 10 条结果；quick 再计 1 + 新增 8 → 总计 10
	if nResults != 10 {
		t.Errorf("second execution should produce 10 results, got %d", nResults)
	}
	if got := counter.Load(); got != 10 {
		t.Errorf("total executions want 10, got %d", got)
	}
}

// TestNestedFlow_SetHookManagerDuringRun_NoPenetration 是 #83 的回归钉：
// #46 的快照契约「与 Run 并发调用是安全的：本次 Run 继续使用快照中的旧
// manager」必须对嵌套层同样成立。修复前 SetHookManager 立即向子流程递归
// 传播（写穿子流程自己的 manager 字段），而子流程稍后启动的执行读的是
// 自己字段的快照——同一 Run 混用两套 hook（外层 hm1、子层 hm2），
// 审计/追踪覆盖撕裂（无数据竞争，-race 抓不到，纯语义违约）。
func TestNestedFlow_SetHookManagerDuringRun_NoPenetration(t *testing.T) {
	newCounting := func() (*HookManager, *mockHook) {
		hm := NewHookManager()
		h := &mockHook{}
		hm.Add(h)
		return hm, h
	}

	subtests := []struct {
		name string
		// child 构造子流程，返回子流程与其内部步骤数
		// （hook 在每层容器/步骤各触发一次）。
		child func() (Runnable, int32)
	}{
		{"pipe", func() (Runnable, int32) {
			c := NewPipe()
			c.Add(&mockRunnable{name: "stepA"}, &mockRunnable{name: "stepB"})
			return c, 2
		}},
		{"async", func() (Runnable, int32) {
			c := NewAsync(0)
			c.Add(&mockRunnable{name: "taskA"}, &mockRunnable{name: "taskB"})
			return c, 2
		}},
		{"switch", func() (Runnable, int32) {
			c := NewSwitch(func(ctx context.Context) string { return "go" })
			c.Case("go", &mockRunnable{name: "branch"})
			return c, 1
		}},
	}
	for _, tc := range subtests {
		t.Run(tc.name, func(t *testing.T) {
			hm1, h1 := newCounting()
			hm2, h2 := newCounting()

			gateTask := newStartedGateTask()
			child, childSteps := tc.child()

			pipe := NewPipe()
			pipe.SetHookManager(hm1)
			pipe.Add(gateTask, child)

			runDone := make(chan error, 1)
			go func() { runDone <- pipe.Run(context.Background()) }()
			<-gateTask.started // 父 Run 已快照 hm1 且执行中

			pipe.SetHookManager(hm2) // 穿透企图：递归写穿子流程字段
			close(gateTask.gate)

			if err := <-runDone; err != nil {
				t.Fatalf("Run: %v", err)
			}
			// 契约预期：本次 Run 全程 hm1（父层 gate+子容器 2 次 + 子层
			// childSteps 次），hm2 本次 Run 一次都不触发
			want := 2 + childSteps
			if got := h1.beforeCount.Load(); got != want {
				t.Errorf("hm1 before=%d (want %d：本次 Run 全程用快照 manager，#83)", got, want)
			}
			if got := h1.afterCount.Load(); got != want {
				t.Errorf("hm1 after=%d (want %d)", got, want)
			}
			if got := h2.beforeCount.Load(); got != 0 {
				t.Errorf("hm2 before=%d (want 0：Run 期间替换的 manager 不得穿透嵌套快照，#83)", got)
			}
			if got := h2.afterCount.Load(); got != 0 {
				t.Errorf("hm2 after=%d (want 0)", got)
			}
		})
	}
}

// TestSwitchFlow_ConcurrentCaseDuringRun 回归测试（#46）：Run 期间并发 Case
// 不再是数据竞争（修复前并发写 map 与 Run 读直接构成竞争，Go runtime 对
// 并发写 map 会 fatal）。本次 Run 执行选中的快照分支，下次 Run 拾取新分支。
func TestSwitchFlow_ConcurrentCaseDuringRun(t *testing.T) {
	key := "main"
	gateTask := newStartedGateTask()
	extraRan := &atomic.Int32{}
	sw := NewSwitch(func(ctx context.Context) string { return key })
	sw.Case("main", gateTask)

	runDone := make(chan error, 1)
	go func() { runDone <- sw.Run(context.Background()) }()
	<-gateTask.started

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			sw.Case(fmt.Sprintf("extra-%d", i), &mockRunnable{name: "extra", counter: extraRan})
		}()
	}
	wg.Wait()
	close(gateTask.gate)

	if err := <-runDone; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := gateTask.runs.Load(); got != 1 {
		t.Errorf("first Run should execute the snapshot branch, got %d runs", got)
	}
	if got := extraRan.Load(); got != 0 {
		t.Errorf("branches added during Run must not execute in that run, got %d", got)
	}

	// 下次 Run 拾取新增分支（key 变更与读取均在测试 goroutine 内先后发生）
	key = "extra-0"
	if err := sw.Run(context.Background()); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if got := extraRan.Load(); got != 1 {
		t.Errorf("second Run should pick up the branch added during first run, got %d", got)
	}
}
