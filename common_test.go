package workflow

import (
	"context"
	"errors"
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

// TestSafeRun_AfterHookPanic_JoinsTaskError 是 #62 的回归钉：任务真实错误
// 与 After hook panic 并存时必须 join 而非整体替换——此前 hook 的 infra
// 噪音顶掉根因错误（errors.Is(err, taskErr) 不再成立），排障主线索被
// 永久丢弃。钉住：任务错误链保留（errors.Is 成立）+ hook panic 仍可见。
func TestSafeRun_AfterHookPanic_JoinsTaskError(t *testing.T) {
	hm := NewHookManager()
	hm.Add(&panickyAfterHook{})

	taskErr := errors.New("payment gateway 503")
	err := safeRun(context.Background(), &errTask{err: taskErr}, hm)
	if err == nil {
		t.Fatal("expected joined error, got nil")
	}
	if !errors.Is(err, taskErr) {
		t.Fatalf("task real error must survive after-hook panic (join, not replace, #62): got %v", err)
	}
	if !strings.Contains(err.Error(), "after hook panic") {
		t.Errorf("after hook panic must still be reported alongside task error, got %v", err)
	}
}

// TestSafeRun_TaskPanicThenAfterHookPanic_Joins 两个 panic 叠加态：任务
// panic 转义的错误与 After hook panic 也必须 join（#62 同根因）。
func TestSafeRun_TaskPanicThenAfterHookPanic_Joins(t *testing.T) {
	hm := NewHookManager()
	hm.Add(&panickyAfterHook{})

	err := safeRun(context.Background(), &panickyTask{}, hm)
	if err == nil {
		t.Fatal("expected joined error, got nil")
	}
	if !strings.Contains(err.Error(), "workflow: panic") {
		t.Errorf("task panic must survive after-hook panic (join, not replace, #62): got %v", err)
	}
	if !strings.Contains(err.Error(), "after hook panic") {
		t.Errorf("after hook panic must still be reported, got %v", err)
	}
}

// panickyTask 在 Run 阶段 panic 的 Runnable（#62 叠加态用例）。
type panickyTask struct{}

func (t *panickyTask) Run(context.Context) error { panic("task boom") }

// TestSafeRun_AfterPanicDoesNotTruncateLaterHooks 是 #81 次生缺陷的回归钉：
// After 循环逐 hook recover——任一 hook 的 After panic 不得截断后续 hook 的
// After（配对进一步撕裂）；panic 聚合为错误、按 #62 口径与任务错误 join。
func TestSafeRun_AfterPanicDoesNotTruncateLaterHooks(t *testing.T) {
	later := &mockHook{}
	hm := NewHookManager()
	hm.Add(&panickyAfterHook{}, later)

	taskErr := errors.New("payment declined")
	err := safeRun(context.Background(), &errTask{err: taskErr}, hm)
	if err == nil {
		t.Fatal("expected joined error, got nil")
	}
	if !errors.Is(err, taskErr) {
		t.Fatalf("task error must survive after-hook panic (join, #62): got %v", err)
	}
	if !strings.Contains(err.Error(), "after hook panic") {
		t.Errorf("after hook panic must be reported, got %v", err)
	}
	if got := later.beforeCount.Load(); got != 1 {
		t.Errorf("later hook before=%d (want 1)", got)
	}
	if got := later.afterCount.Load(); got != 1 {
		t.Errorf("later hook after=%d (want 1：After panic 不得截断后续 hook 的 After，#81)", got)
	}
}
