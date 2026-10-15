package workflow

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/oekit/logx"
)

// flowLog 是 workflow 包专用 logger(hook 覆盖告警等)。
var flowLog = logx.Delegating("workflow")

// isCancellationErr 判断错误链是否包含 context 取消/超时。
func isCancellationErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
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

// safeRun 执行 Hook + Runnable，捕获 panic 转为 error。
// 与 ParallelFlow/AsyncFlow 的 goSafeExecute 行为一致。
// After hook 在 Run 正常返回或 panic 时都会触发（通过 defer 保证）。
func safeRun(ctx context.Context, r Runnable, hm *HookManager) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("workflow: panic: %v\n%s", r, debug.Stack())
		}
		// After hook 在 panic 时也触发（defer 保证执行）。
		// 必须自带 recover：此时外层 recover 已被上面的消费耗尽，After 再
		// panic 会炸穿 Run（Pipe 路径）；在 GoSafe goroutine 里则被顶层
		// recover 吞掉且 resultFn 永不执行——Parallel/Async 静默丢任务错误。
		defer func() {
			if rp := recover(); rp != nil {
				err = fmt.Errorf("workflow: after hook panic: %v\n%s", rp, debug.Stack())
			}
		}()
		if hm != nil && hm.Len() > 0 {
			hm.After(ctx, r, err)
		}
	}()

	if hm != nil && hm.Len() > 0 {
		// Before hook 拒绝执行，After hook 仍需触发（defer 保证）
		if berr := hm.Before(ctx, r); berr != nil {
			return berr
		}
	}

	err = r.Run(ctx)
	return err
}
