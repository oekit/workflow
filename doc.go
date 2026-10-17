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
// 任务错误采用统一豁免规则：
//
//   - 流程级 context 确实被取消/超时 → 视为取消的副作用，不上报；
//   - 流程级 context 健康 → 视为任务自身的失败（例如任务内部子操作超时
//     返回 fmt.Errorf("db timeout: %w", context.DeadlineExceeded)），必须
//     正常上报给调用方，绝不吞错。
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
