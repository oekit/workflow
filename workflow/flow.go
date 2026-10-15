package workflow

// Flow 流程容器接口。
// 继承 Runnable，支持组合嵌套。
// SetHookManager 用于递归传播 HookManager 到子流程。
// Hooks 返回当前 manager(传播前据此检测子流程自有 hook 被覆盖并告警)。
type Flow interface {
	Runnable
	Hooks() *HookManager
	SetHookManager(*HookManager)
}
