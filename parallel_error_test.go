package workflow

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestParallelFlow_Run_CancelReturnsBeforeNonCooperativeTaskFinishes 是 #106
// 的回归钉（与 AsyncFlow 的 #80 回归钉 async_error_test.go 同形，挂在
// ParallelFlow 上）：契约「阻塞直到全部完成或 context 取消」——ctx 取消后
// Run 只留有界收尾窗口（runCancelDrainGrace，#61 的真实错误浮出用），不得
// 无界等待不合作任务自然返回。修复前收尾是 Run goroutine 上裸的
// `for err := range errs`（errs 由 done 关闭驱动）：任务 goroutine 不收尾
// Run 就不返回，任务挂多久调用方的关停流程挂多久（#80 定性为「契约中
// 『context 取消』一侧失效」，ParallelFlow 是同族缺陷的另一个 Flow 面）。
// 定序全走屏障（started 信号 + 永不关闭的 block channel，t.Cleanup 放行
// 后台收尾），不 sleep 猜「任务已启动」。
func TestParallelFlow_Run_CancelReturnsBeforeNonCooperativeTaskFinishes(t *testing.T) {
	block := make(chan struct{}) // 任务收尾完全由测试控制：断言前绝不放行
	started := make(chan struct{})
	p := NewParallel(2)
	p.Add(funcRunnable(func(context.Context) error {
		close(started)
		<-block // 不合作任务：不看 ctx，模拟未接 ctx 的阻塞调用
		return nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- p.Run(ctx) }()

	<-started // 任务已阻塞在 block 上（此后对 ctx 取消无动于衷）
	cancel()  // 优雅关停

	select {
	case err := <-runErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second): // runCancelDrainGrace(500ms) 的大量级裕量
		t.Fatal("Run did not return within the bounded drain window：不合作任务让 Run 挂死（#106）")
	}
	t.Cleanup(func() { close(block) }) // 断言完毕后放行后台收尾
}

// TestParallelFlow_Run_RealErrorSurfacesWithinGraceAfterCancel 钉住 #106 的
// #61 语义不回退面：ctx 取消后，宽限窗口内收尾的任务的真实错误仍要浮出
// （不得被取消错误掩蔽）——与 AsyncFlow.Run 同场景语义对齐（先 drain 完
// 任务结果再看 ctx）。
func TestParallelFlow_Run_RealErrorSurfacesWithinGraceAfterCancel(t *testing.T) {
	realErr := errors.New("payment gateway 503")
	started := make(chan struct{})
	release := make(chan struct{})
	p := NewParallel(2)
	p.Add(funcRunnable(func(context.Context) error {
		close(started)
		<-release // 先阻塞；取消后放行，真实错误在宽限窗口内浮出
		return realErr
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- p.Run(ctx) }()

	<-started
	cancel()
	close(release) // 任务随即收尾：窗口内完成，真实错误必须浮出

	select {
	case err := <-runErr:
		if !errors.Is(err, realErr) {
			t.Fatalf("real task error masked by ctx cancellation: got %v, want %v (#61/#106)", err, realErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return within budget")
	}
}

// TestParallelFlow_Run_CooperativeTasksUnboundedByGrace 钉住 #106 的另一半
// 边界：正常路径（context 健康的长任务）不受 runCancelDrainGrace 约束——
// Run 仍阻塞到全部任务完成，宽限窗口只在「context 已取消」的收尾面生效。
func TestParallelFlow_Run_CooperativeTasksUnboundedByGrace(t *testing.T) {
	p := NewParallel(1)
	p.Add(funcRunnable(func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1200 * time.Millisecond): // > runCancelDrainGrace(500ms)
			return nil
		}
	}))

	start := time.Now()
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run error: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 1200*time.Millisecond {
		t.Fatalf("Run returned in %v — healthy-context tasks must not be bounded by the drain grace (#106)", elapsed)
	}
}

// --- #149: 豁免判据/首错聚合单源 errAggregator 的 ParallelFlow 回归钉 ---

// TestParallelFlow_TaskErrorWrappedWithTaskIndex 钉住 #149 收敛后唯一保留的
// 出口差异：ParallelFlow 的错误面仍经 pkgerrors.Wrapf 标注任务序号
// （"workflow: parallel task N failed"），errors.Is 穿透包装链直达任务原始
// 错误——errs channel 删除后包装差异不得丢失。
func TestParallelFlow_TaskErrorWrappedWithTaskIndex(t *testing.T) {
	realErr := errors.New("db constraint violated")
	p := NewParallel(2)
	p.Add(&errTask{err: realErr})

	err := p.Run(context.Background())
	if err == nil {
		t.Fatal("expected task error, got nil")
	}
	if !errors.Is(err, realErr) {
		t.Fatalf("errors.Is must pierce the wrap chain, got %v", err)
	}
	if !strings.Contains(err.Error(), "workflow: parallel task 0 failed") {
		t.Errorf("task-index wrap prefix lost after #149 migration: %q", err.Error())
	}
}

// TestParallelFlow_Run_ParentCancelAndRealError_SurfacesRealError 是 #149
// 判据统一 + #61 语义对齐的回归钉（与 AsyncFlow 同名用例同形）：真实错误
// 已入账（record → firstErr）、内部 cancel 已触发、gate 任务经取消收尾
// （取消副作用走豁免）后，调用方父 ctx 也被取消——Run 必须确定性返回真实
// 任务错误，不得被 context.Canceled 掩蔽（outcome 的 firstErr 优先级，
// 不掷硬币）。
func TestParallelFlow_Run_ParentCancelAndRealError_SurfacesRealError(t *testing.T) {
	const rounds = 30
	realErr := errors.New("payment gateway 503")
	for i := 0; i < rounds; i++ {
		ran := make(chan struct{}, 1)
		gate := make(chan struct{})
		p := NewParallel(2)
		p.Add(
			&failFastTask{ran: ran, err: realErr}, // A：快速真实失败 → firstErr → 流程 cancel
			&gateRunnable{gate: gate},             // B：阻塞至取消收尾 → 豁免路径
		)

		parent, pcancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- p.Run(parent) }()

		<-ran     // A 已失败：firstErr 已入账、流程 cancel 已触发，B 仍阻塞
		pcancel() // 父 ctx 取消：B 解除阻塞走豁免，真实错误不得被掩蔽

		select {
		case err := <-errCh:
			if !errors.Is(err, realErr) {
				t.Fatalf("round %d: real task error masked by ctx cancellation: got %v, want %v (#61/#149)", i+1, err, realErr)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: Run did not return within budget", i+1)
		}
	}
}

// TestParallelFlow_Run_CallerCancelSideEffectExemptedAndNoFakeSuccess 钉住
// 判据统一的豁免面（#149，errAggregator.exempted 条件 1+2）：调用方取消
// 在途后，任务自报的 context.Canceled 是取消的副作用——豁免（不得以
// "parallel task N failed" 包装错误形态返回），但 Run 仍不得假成功，返回
// context.Canceled（#35 兜底，errAggregator.outcome 单源）。收尾不走
// select（与 ctx.Done 掷硬币），时序全由屏障控制。
func TestParallelFlow_Run_CallerCancelSideEffectExemptedAndNoFakeSuccess(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	p := NewParallel(1)
	p.Add(funcRunnable(func(ctx context.Context) error {
		close(started)
		<-release
		if err := ctx.Err(); err != nil {
			return err // 取消副作用：任务自报取消错误
		}
		return nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- p.Run(ctx) }()

	<-started
	cancel()       // 调用方取消：任务随后自报的 Canceled 属取消副作用
	close(release) // 放行收尾：宽限窗口内完成

	select {
	case err := <-runErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled (#35 取消不得假成功), got %v", err)
		}
		if strings.Contains(err.Error(), "parallel task") {
			t.Fatalf("取消副作用须豁免，不得以包装任务错误形态返回, got %q", err.Error())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return within budget")
	}
}
