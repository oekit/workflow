package workflow

import (
	"context"
	"fmt"
)

// SwitchFunc 条件路由函数。
// 根据 context 返回要执行的分支 key。
type SwitchFunc func(ctx context.Context) string

// SwitchFlow 条件分支流程。
// 根据 SwitchFunc 返回的 key 选择执行路径。
//
// 并发契约（#46）：Case/Hooks/SetHookManager 内部加锁，与 Run 并发调用
// 不构成数据竞争。Run 在选定 key 后以加锁读取瞬间的分支表执行，Run
// 期间新增的分支不影响本次执行（下次 Run 生效）。
type SwitchFlow struct {
	flowHooks
	runnables  map[string]Runnable
	switchFunc SwitchFunc
}

// NewSwitch 创建一个条件分支流程。
func NewSwitch(switchFunc SwitchFunc) *SwitchFlow {
	s := &SwitchFlow{
		runnables:  make(map[string]Runnable),
		switchFunc: switchFunc,
	}
	s.hookManager = NewHookManager()
	return s
}

// Case 添加一个分支。
// 重复 key 后者覆盖前者（静默）。
// 与 Run 并发调用是安全的：本次 Run 不感知并发新增的分支（快照语义）。
// 若分支本身是子流程,当前 HookManager 会同步传播给它(2026-09-06 复审修复)。
func (s *SwitchFlow) Case(key string, runnable Runnable) *SwitchFlow {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runnables[key] = runnable
	setHookManagerByRunnable(runnable, s.hookManager)
	return s
}

// SetHookManager 设置 HookManager（用于嵌套传播）。
// 与 Run 并发调用是安全的：本次 Run 继续使用加锁读取到的旧 manager。
// 递归传播到子流程（成员是 map，传播循环无法与切片型 Flow 共用，#94）。
func (s *SwitchFlow) SetHookManager(hm *HookManager) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hookManager = hm
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
	s.mu.Lock()
	hm := s.hookManager
	s.mu.Unlock()
	return s.runWithHooks(ctx, hm)
}

// runWithHooks 以显式 HookManager 执行本次流程（#83，见 PipeFlow 同名
// 方法注释）：嵌套执行用父流程快照的 manager，不受 SetHookManager 穿透。
func (s *SwitchFlow) runWithHooks(ctx context.Context, hm *HookManager) error {
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

	// 查找分支（#46：加锁读取分支与 HookManager 的瞬时快照，
	// 与并发 Case/SetHookManager 互不构成数据竞争）
	runnable, ownHM, exists := s.branch(key)
	if !exists {
		return fmt.Errorf("workflow: no such branch: %s", key)
	}
	if hm == nil {
		hm = ownHM
	}

	return safeRun(ctx, runnable, hm)
}

// branch 加锁读取 key 对应的分支与当前 HookManager。
func (s *SwitchFlow) branch(key string) (runnable Runnable, hm *HookManager, exists bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	runnable, exists = s.runnables[key]
	return runnable, s.hookManager, exists
}

// selectBranch 调 switchFunc 并捕获 panic（#97：经 pkg errors.Recovered
// 单源收编，panic 值是 error 时身份透传）。
func (s *SwitchFlow) selectBranch(ctx context.Context) (key string, err error) {
	defer func() {
		if r := recover(); r != nil {
			key = ""
			err = panicError("switch func panic", r)
		}
	}()
	return s.switchFunc(ctx), nil
}
