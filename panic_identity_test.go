package workflow

import (
	"context"
	"errors"
	"strings"
	"testing"

	pkgerrors "github.com/oekit/pkg/errors"
)

// panicErrTask panic 出一个 error 值（业务侧常见形态）：收编后 error 身份
// 必须透传（#97：此前 fmt.Errorf(%v) 把 panic 值拍平成纯文本，身份丢失）。
type panicErrTask struct {
	sentinel error
}

func (t *panicErrTask) Run(context.Context) error { panic(t.sentinel) }

// panicBeforeHook / panicAfterHook 在 Before/After 阶段 panic 出 error 的钩子。
type panicBeforeHook struct {
	sentinel error
}

func (h *panicBeforeHook) Before(context.Context, Runnable) error { panic(h.sentinel) }
func (h *panicBeforeHook) After(context.Context, Runnable, error) {}

type panicAfterHook struct {
	sentinel error
}

func (h *panicAfterHook) Before(context.Context, Runnable) error { return nil }
func (h *panicAfterHook) After(context.Context, Runnable, error) { panic(h.sentinel) }

// assertPanicCarrier 校验错误链同时满足 #97 的两个承诺：panic 值是 error
// 时身份透传（errors.Is 穿透），且收编载体可被 errors.As 取回（栈在场）。
func assertPanicCarrier(t *testing.T, err error, sentinel error) {
	t.Helper()
	if !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is must pierce the panic carrier to the original error (#97): got %v", err)
	}
	var perr *pkgerrors.PanicError
	if !errors.As(err, &perr) {
		t.Fatalf("errors.As must find *pkgerrors.PanicError in the chain: got %v", err)
	}
	if len(perr.Stack) == 0 {
		t.Error("PanicError.Stack must be captured")
	}
}

// TestPanicRecovery_ErrorIdentitySurvives_Pipe 是 #97 的回归钉：Runnable
// 以 panic 携带 error 时（业务侧常见形态），PipeFlow.Run 的错误面必须
// 保留身份——调用方仍能 errors.Is 判定根因、errors.As 取回带栈的收编载体。
func TestPanicRecovery_ErrorIdentitySurvives_Pipe(t *testing.T) {
	sentinel := errors.New("disk full")
	pipe := NewPipe()
	pipe.Add(&panicErrTask{sentinel: sentinel})

	err := pipe.Run(context.Background())
	if err == nil {
		t.Fatal("expected error from panicked task")
	}
	assertPanicCarrier(t, err, sentinel)
}

// TestPanicRecovery_ErrorIdentitySurvives_AsyncRun 同一根因的 goroutine
// 路径（goSafeExecute → safeRun）：AsyncFlow.Run 收编后身份同样透传。
func TestPanicRecovery_ErrorIdentitySurvives_AsyncRun(t *testing.T) {
	sentinel := errors.New("disk full")
	a := NewAsync(2)
	a.Add(&panicErrTask{sentinel: sentinel})

	if err := a.Run(context.Background()); !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is must pierce the panic carrier through AsyncFlow.Run (#97): got %v", err)
	}
}

// TestPanicRecovery_NonErrorValue_StillCarrierWithStack 非 error 的 panic
// 值（string 等）同样收编为载体：类型信息进错误文本（"panic: %T: %v"）、
// 栈在场——文本尾部保留 goroutine 栈（运维面契约，此前 %v\n%s 直接拼栈）。
func TestPanicRecovery_NonErrorValue_StillCarrierWithStack(t *testing.T) {
	pipe := NewPipe()
	pipe.Add(&panickyTask{}) // panic("task boom")

	err := pipe.Run(context.Background())
	if err == nil {
		t.Fatal("expected error from panicked task")
	}
	var perr *pkgerrors.PanicError
	if !errors.As(err, &perr) {
		t.Fatalf("errors.As must find *pkgerrors.PanicError: got %v", err)
	}
	if len(perr.Stack) == 0 {
		t.Error("PanicError.Stack must be captured")
	}
	// 类型信息在场（此前只有 %v 的纯值文本）
	if !strings.Contains(err.Error(), "panic: string: task boom") {
		t.Errorf("error text must carry panic value type info, got %v", err)
	}
	// 栈保留在错误文本尾部（对齐既有运维习惯：日志里 grep goroutine 栈）
	if !strings.Contains(err.Error(), "goroutine ") {
		t.Errorf("error text must keep the captured stack, got %v", err)
	}
}

// TestHookBeforePanic_ErrorIdentitySurvives #97 的 hook 路径之一：Before
// panic 携带 error，safeRun 返回的错误身份透传。
func TestHookBeforePanic_ErrorIdentitySurvives(t *testing.T) {
	hookSentinel := errors.New("auth backend unreachable")
	hm := NewHookManager()
	hm.Add(&panicBeforeHook{sentinel: hookSentinel})

	err := safeRun(context.Background(), &okTask{}, hm)
	if err == nil {
		t.Fatal("expected error from panicked before hook")
	}
	assertPanicCarrier(t, err, hookSentinel)
}

// TestHookAfterPanic_ErrorIdentitySurvives #97 的 hook 路径之二：After
// panic 携带 error 且任务真实错误并存——join 后两个身份都必须可达
// （#62 join 口径与 #97 身份透传叠加）。
func TestHookAfterPanic_ErrorIdentitySurvives(t *testing.T) {
	hookSentinel := errors.New("metrics exporter down")
	taskSentinel := errors.New("payment declined")
	hm := NewHookManager()
	hm.Add(&panicAfterHook{sentinel: hookSentinel})

	err := safeRun(context.Background(), &errTask{err: taskSentinel}, hm)
	if err == nil {
		t.Fatal("expected joined error")
	}
	if !errors.Is(err, taskSentinel) {
		t.Fatalf("task error identity must survive the join (#62): got %v", err)
	}
	assertPanicCarrier(t, err, hookSentinel)
}

// TestSwitchFuncPanic_ErrorIdentitySurvives #97 的第 5 处转换点：分支
// 函数 panic 携带 error，SwitchFlow.Run 的错误面身份透传。
func TestSwitchFuncPanic_ErrorIdentitySurvives(t *testing.T) {
	sentinel := errors.New("routing table corrupt")
	sw := NewSwitch(func(context.Context) string { panic(sentinel) })
	sw.Case("a", &okTask{})

	err := sw.Run(context.Background())
	if err == nil {
		t.Fatal("expected error from panicked switch func")
	}
	assertPanicCarrier(t, err, sentinel)
}
