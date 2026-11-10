// Package workflow 提供组合式流程编排框架。
//
// 支持串行、并行、异步、条件分支四种流程模式，可任意嵌套组合。
// 所有流程节点共享 Runnable 接口，Hook 自动递归传播实现 AOP。
//
// # 流程类型
//
//   - PipeFlow: 串行流程，按顺序依次执行，任一失败则停止
//   - ParallelFlow: 并行流程，并发执行，任一失败则取消其他
//   - AsyncFlow: 异步流程，返回 Future，不阻塞调用方
//   - SwitchFlow: 条件分支，根据 key 选择执行路径
//
// # 组合嵌套
//
//	pipe := workflow.NewPipe()
//	pipe.Add(step1, step2)
//
//	parallel := workflow.NewParallel(10)
//	parallel.Add(task1, task2)
//
//	// 嵌套：串行流程中包含并行流程
//	pipe.Add(parallel)
//
// # Context 取消传播
//
// 所有流程支持 context 取消传播：
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	// 如果 ctx 被取消，所有正在执行的任务会收到取消信号
//	err := pipe.Run(ctx)
//
// # 任务错误与 context 取消的语义
//
// ParallelFlow/AsyncFlow 对错误链包含 context.Canceled/DeadlineExceeded 的
// 任务错误采用统一豁免规则，判据是调用方传入的流程级 context（不是框架
// 派生、会被框架在任务失败时 cancel 的内部 context，#82）：
//
//   - 调用方 context 确实被取消/超时 → 视为取消的副作用，不上报；
//   - 调用方 context 健康 → 视为任务自身的失败（例如任务内部子操作超时
//     返回 fmt.Errorf("db timeout: %w", context.DeadlineExceeded)），必须
//     正常上报给调用方，绝不吞错。
//
// 由此，兄弟任务失败触发的框架内部取消不豁免：被砍/未执行的任务在
// AsyncFlow.RunAsyncWithResults 结果通道里报 context.Canceled 类错误，与
// 「确实跑完」（Error: nil）可区分；被豁免（调用方取消）的任务结果记为
// Error: nil（每个任务恰好一条结果，Index 连续）。「通道内全部 Error 为
// nil」不代表流程未被取消，调用方需结合自身 context 判断。
//
// # 并发契约
//
// 各 Flow 的 Add/Case、Hooks、SetHookManager 内部加锁，与 Run 并发调用
// 不构成数据竞争。Run 以启动瞬间的成员快照执行：执行期间新增的任务/分支、
// 替换的 HookManager 不影响本次 Run，下次 Run 生效。
// 注意 HookManager 自身不在此列——Run 期间向 HookManager 注册 Hook 仍是
// 数据竞争，Hook 必须在 Run 之前注册完毕（见 HookManager 并发契约）。
//
// # Hook AOP
//
//	hook := workflow.NewHookManager()
//	hook.Add(&LogHook{}, &MetricsHook{})
//
//	// Hook 会自动递归传播到子流程
//	pipe.SetHookManager(hook)
//
// # 异步任务
//
//	async := workflow.NewAsync(10)
//	async.Add(task1, task2, task3)
//
//	// 返回 Future，不阻塞
//	future := async.RunAsync(ctx)
//
//	// 等待完成或取消
//	err := future.Wait(ctx)
//	future.Cancel()
package workflow
