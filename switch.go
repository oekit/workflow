package workflow

import (
	"context"
	"fmt"
	"runtime/debug"
)

// SwitchFunc 条件路由函数。
// 根据 context 返回要执行的分支 key。
type SwitchFunc func(ctx context.Context) string

// SwitchFlow 条件分支流程。
// 根据 SwitchFunc 返回的 key 选择执行路径。
type SwitchFlow struct {
	runnables   map[string]Runnable
	switchFunc  SwitchFunc
	hookManager *HookManager
}

// NewSwitch 创建一个条件分支流程。
func NewSwitch(switchFunc SwitchFunc) *SwitchFlow {
	return &SwitchFlow{
		runnables:   make(map[string]Runnable),
		switchFunc:  switchFunc,
		hookManager: NewHookManager(),
	}
}

// Case 添加一个分支。
// 重复 key 后者覆盖前者（静默）；分支集合应在 Run 前定型。
// 若分支本身是子流程,当前 HookManager 会同步传播给它(2026-09-06 复审修复)。
func (s *SwitchFlow) Case(key string, runnable Runnable) *SwitchFlow {
	s.runnables[key] = runnable
	setHookManagerByRunnable(runnable, s.hookManager)
	return s
}

// Hooks 获取 Hook 管理器。
func (s *SwitchFlow) Hooks() *HookManager {
	return s.hookManager
}

// SetHookManager 设置 HookManager（用于嵌套传播）。
func (s *SwitchFlow) SetHookManager(hm *HookManager) {
	s.hookManager = hm
	// 递归传播到子流程
	for _, r := range s.runnables {
		setHookManagerByRunnable(r, hm)
	}
}

// Run 根据 SwitchFunc 选择并执行分支。
// 如果 SwitchFunc 返回的 key 不存在，返回错误。
// SwitchFunc 自身的 panic 同样被捕获转为 error（与 PipeFlow 的
// "Hook 或 Runnable 的 panic 会被捕获"契约对齐,2026-09-06 复审修复:
// 原实现裸调 switchFunc,分支选择函数 panic 直接炸穿调用方）。
func (s *SwitchFlow) Run(ctx context.Context) error {
	// 检查 context
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("workflow: cancelled: %w", err)
	}

	// 检查 SwitchFunc
	if s.switchFunc == nil {
		return fmt.Errorf("workflow: switch function not set")
	}

	// 获取分支 key（panic 转 error）
	key, err := s.selectBranch(ctx)
	if err != nil {
		return err
	}

	// 查找分支
	runnable, exists := s.runnables[key]
	if !exists {
		return fmt.Errorf("workflow: no such branch: %s", key)
	}

	return safeRun(ctx, runnable, s.hookManager)
}

// selectBranch 调 switchFunc 并捕获 panic。
func (s *SwitchFlow) selectBranch(ctx context.Context) (key string, err error) {
	defer func() {
		if r := recover(); r != nil {
			key = ""
			err = fmt.Errorf("workflow: switch func panic: %v\n%s", r, debug.Stack())
		}
	}()
	return s.switchFunc(ctx), nil
}
