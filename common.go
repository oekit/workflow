package workflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/oekit/logx"
	pkgerrors "github.com/oekit/pkg/errors"
)

// flowLog 是 workflow 包专用 logger(hook 覆盖告警等)。
var flowLog = logx.Delegating("workflow")

// isCancellationErr 判断错误链是否包含 context 取消/超时。
func isCancellationErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// panicError 把 recover() 的返回值收编为 error（#97）：统一走 pkg
// errors.Recovered 单源——panic 值本身是 error 时身份透传（errors.Is /
// errors.As 穿透 *errors.PanicError 直达原始错误），goroutine 栈保留在
// 错误文本尾部与 PanicError.Stack 字段两处。此前 5 处
// fmt.Errorf("...: %v\n%s", rec, debug.Stack()) 把 panic 值拍平成纯文本，
// error 身份当场丢失。where 标注 panic 发生的位置（panic / before hook
// panic / after hook panic / switch func panic），沿用既有错误文本前缀。
// 调用方必须处于 `if rec := recover(); rec != nil` 守卫内（rec 非 nil）。
func panicError(where string, rec any) error {
	perr := pkgerrors.Recovered(rec)
	// Recovered 对非 nil 入参恒返回 *PanicError（单源契约）；断言仅为取
	// Stack 字段渲染进文本。
	pe, ok := perr.(*pkgerrors.PanicError)
	if !ok {
		return perr
	}
	return fmt.Errorf("workflow: %s: %w\n%s", where, pe, pe.Stack)
}

// setHookManagerByRunnable 根据 Runnable 类型设置 HookManager。
// 如果 Runnable 是 Flow，递归设置 HookManager。
//
// 2026-09-06 复审加固:子流程自带 hook(manager 非空且非目标 manager)被
// 覆盖时打 Warn——原实现静默丢弃,与 doc "Hook 自动递归传播"的承诺之间
// 没有任何可见信号。注意传播时序契约:SetHookManager 与 Add 都会向子流程
// 传播当前 manager,两种调用顺序下 hook 均生效。
func setHookManagerByRunnable(runnable Runnable, hm *HookManager) {
	flow, ok := runnable.(Flow)
	if !ok {
		return
	}
	if cur := flow.Hooks(); cur != nil && cur != hm && cur.Len() > 0 {
		flowLog.Warn("child flow has its own hooks; they will be shadowed by the parent hook manager",
			"child_type", fmt.Sprintf("%T", runnable),
			"shadowed_hooks", cur.Len())
	}
	flow.SetHookManager(hm)
}

// hookScopedRunner 是「以显式 HookManager 执行」的内部接口（#83）。
// 嵌套执行时父流程把自己快照中的 manager 传给子流程的本次执行，子流程
// 不读自身字段——Run 期间 SetHookManager 的递归传播无法穿透正在进行的
// 执行（#46 快照契约对嵌套层同样成立）。
type hookScopedRunner interface {
	runWithHooks(ctx context.Context, hm *HookManager) error
}

// safeRun 执行 Hook + Runnable，捕获 panic 转为 error。
// 与 ParallelFlow/AsyncFlow 的 goSafeExecute 行为一致。
// After hook 在 Run 正常返回或 panic 时都会触发（通过 defer 保证），
// 且只对 Before 已执行的 hook 配对触发（#81）。
func safeRun(ctx context.Context, r Runnable, hm *HookManager) (err error) {
	beforeN := 0 // 已执行 Before 的 hook 数（#81 配对信息）
	defer func() {
		if rec := recover(); rec != nil {
			err = panicError("panic", rec)
		}
		// After hook 在 panic 时也触发（defer 保证执行）。
		// 必须自带 recover：此时外层 recover 已被上面的消费耗尽，After 再
		// panic 会炸穿 Run（Pipe 路径）；在 GoSafe goroutine 里则被顶层
		// recover 吞掉且 resultFn 永不执行——Parallel/Async 静默丢任务错误。
		defer func() {
			if rp := recover(); rp != nil {
				hookErr := panicError("after hook panic", rp)
				// #62：任务真实错误与 hook panic 并存时 join 而非整体替换——
				// 此前 hook 的 infra 噪音顶掉根因错误（errors.Is(err, taskErr)
				// 不再成立），排障时主线索被永久丢弃。
				err = joinHookErr(err, hookErr)
			}
		}()
		if beforeN > 0 {
			// After 只对 Before 已执行的 hook 配对触发（#81）：Before 短路
			// 后未执行 Before 的 hook 不得收到 After（计数/追踪型 Hook 计数
			// 漂移、span 关闭错位）。
			if hookErr := hm.afterRun(ctx, r, err, beforeN); hookErr != nil {
				err = joinHookErr(err, hookErr)
			}
		}
	}()

	if hm != nil && hm.Len() > 0 {
		// Before hook 拒绝执行：拒绝者自身的 After 仍需配对触发（defer
		// 保证），Before 未执行的 hook 不会收到 After（#81）。
		var berr error
		beforeN, berr = hm.beforeRun(ctx, r)
		if berr != nil {
			return berr
		}
	}

	// 嵌套子流程以显式 manager 执行（#83）：本次执行的 hook 全程用父流程
	// 快照中的 manager，不受执行期间 SetHookManager 递归传播的穿透影响。
	if hr, ok := r.(hookScopedRunner); ok {
		err = hr.runWithHooks(ctx, hm)
		return err
	}

	err = r.Run(ctx)
	return err
}

// joinHookErr 把 hook 阶段的错误按 #62 口径并入任务错误：任务真实错误与
// hook panic 并存时 join 而非整体替换——hook 的 infra 噪音不得顶掉根因
// 错误（errors.Is(err, taskErr) 必须继续成立），排障主线索不能被丢弃。
func joinHookErr(err, hookErr error) error {
	if err != nil {
		return errors.Join(err, hookErr)
	}
	return hookErr
}
