package workflow

import (
	"context"
	"errors"
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
