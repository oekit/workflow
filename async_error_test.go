package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
