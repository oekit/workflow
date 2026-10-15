package workflow

import (
	"context"
	"strings"
	"testing"
)

// okTask 恒成功的 Runnable，ran 非空时记录执行。
type okTask struct {
	ran *bool
}

func (t *okTask) Run(context.Context) error {
	if t.ran != nil {
		*t.ran = true
	}
	return nil
}

// panickyAfterHook 在 After 阶段 panic 的钩子。
type panickyAfterHook struct{}

func (h *panickyAfterHook) Before(context.Context, Runnable) error { return nil }
func (h *panickyAfterHook) After(context.Context, Runnable, error) { panic("after hook boom") }

// TestSafeRun_AfterHookPanic 回归：safeRun 外层 recover 消费后调 After——
// After 再 panic 时已无 recover 保护：Pipe 路径炸穿调用方；GoSafe 路径被
// 顶层吞掉且 resultFn 永不执行（Parallel/Async 静默丢任务错误）。
// 钉住：After panic 被收编转为 error。
func TestSafeRun_AfterHookPanic(t *testing.T) {
	hm := NewHookManager()
	hm.Add(&panickyAfterHook{})

	err := safeRun(context.Background(), &okTask{}, hm)
	if err == nil {
		t.Fatal("After hook panic must be converted to error, not propagate")
	}
	if !strings.Contains(err.Error(), "after hook panic") {
		t.Errorf("error should mention after hook panic, got %v", err)
	}
}
