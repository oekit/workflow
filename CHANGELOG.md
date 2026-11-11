# Changelog — oekit/workflow

由 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来。所有 notable 变更记录如下。

## [v0.10.1] — 2026-11-11

> ⚠️ **误贴 tag，按「接受为最新」处理**：v0.10.1 这个版本号系批量发版脚本串仓取号误贴（network 的版本号打进了本仓，与 matrix-tools#290 幽灵 tag v0.2.12 同源 bug），tag 钉在本仓 main HEAD（8aff3af）。protect-release-tags 禁删，版本线接受 v0.4.0 → v0.10.1，后续跟版从 v0.10.1 起。本段补记该 tag 实际包含的内容（v0.4.0 之后的文档同步轮）：

- docs: 矩阵表同步——DESC 31 合 1（config/tarx/resumable 收编，pkg#329/#326/#361）（#90）
- docs(agents): AGENTS.md 首行 DESC 同步矩阵真相源（#88）
- build: 矩阵依赖地板对齐（v0.33.0）（#87）


## [v0.4.0] — 2026-11-11

审计清扫轮（#80–#84）。定版 v0.4.0 而非 v0.3.13：#82 补充新增公开 API——结果三态哨兵 `ErrSkipped`（区分「未执行」，`errors.Is` 判定）；另有 #84 的 After 回放顺序契约变更（0.x minor 允许 breaking）。

- fix(async): Run 的取消后收尾 drain 加上界——ctx 取消后最多再等 runCancelDrainGrace（500ms）的收尾窗口以浮出真实错误（#61 语义不回退），超时即按契约返回取消错误（#80）——此前收尾是裸 `<-future.done`，要等全部任务 goroutine 收尾，不合作任务（不响应 ctx 的阻塞）挂多久 Run 就挂多久，「阻塞直到全部完成或 context 取消」契约的「context 取消」一侧失效

关联问题单：oekit/workflow#80。

- fix(hook): After 与 Before 逐 hook 配对——Before 短路后未执行 Before 的 Hook 不再收到 After，且 After 循环逐 hook recover（#81）——此前 After 无条件遍历全部 hook，注册序 [鉴权(拒绝), 追踪] 时追踪 Hook 收到无配对的 After，计数/追踪型 Hook 计数漂移、span 关闭错位；任一 hook 的 After panic 还会截断后续 hook 的 After，现 panic 聚合为 error 与任务错误 join（#62 口径不变）

关联问题单：oekit/workflow#81。

- fix(async): 取消类错误豁免判据改测调用方 ctx——兄弟失败砍掉的任务不再记 Error:nil，报 context.Canceled 与「确实跑完」可区分；任务自身的超时错误不再被兄弟失败连带吞掉（#82）——此前四处判据测的是框架派生、会被框架在任务失败时 cancel 的 childCtx，退化成「此刻有没有兄弟失败过」，doc「绝不吞错」形态被吞；行为变更：RunAsyncWithResults 中被砍/未执行任务的 Error 从 nil 变为 context.Canceled 类错误（0.x breaking，消费方按 `Error: nil = 成功` 判定的口径收紧）。**新增公开哨兵 `ErrSkipped`**：未开始即被跳过的任务错误链携带该哨兵（`fmt.Errorf` 多 `%w` 与 ctx 取消错误双链，`errors.Is` 判定）——三态「成功 / 失败 / 未执行」显式可辨，`Error: nil` 回归「确实成功」单义；调用方 ctx 主动取消的豁免语义不变（#45，ErrSkipped 仅在调用方 ctx 健康时出现）

关联问题单：oekit/workflow#82。

- fix(pipe): Run 期间 SetHookManager 不再穿透嵌套快照契约——子流程本次执行以父流程快照的 manager 运行，同一 Run 不再混用两套 hook（#83）——此前 SetHookManager 立即递归写穿子流程的 manager 字段，而子流程稍后启动的执行读自己字段的快照，#46「本次 Run 继续使用快照中的旧 manager」的承诺只对直接层成立（AsyncFlow/SwitchFlow 同构，一并修复）

关联问题单：oekit/workflow#83。

- **BREAKING** fix(hook): After 改按逆注册序执行——AOP 洋葱配对恢复，span/事务类嵌套 hook 不再交叉关闭（#84）——此前 Before 正序、After 也正序，外层 hook 的收尾先于内层执行（TracingHook 先关 span 导致 AuditHook 事件丢失/挂错 parent，事务型 hook 的 COMMIT 先于内层收尾）；文档原明写「按注册序调用 After」属契约设计缺陷，本版改为逆序并同步 hook_manager.go/hook.go/doc.go 文案（0.x breaking：依赖 After 正序的消费方需反转收尾逻辑）

关联问题单：oekit/workflow#84。

现代语言特性：`fmt.Errorf` 多 `%w`（ErrSkipped 与 ctx 错误双链）、`errors.Join`（hook panic 聚合）；本轮回归测试的 #80/#82 用例定序全部改为屏障/事件序（started 信号 + gate channel），不再 sleep 猜时序。

关联问题单：oekit/workflow#80、oekit/workflow#81、oekit/workflow#82、oekit/workflow#83、oekit/workflow#84。

## [v0.3.12] — 2026-11-10

- - build: 依赖地板对齐（第二轮发版后 floor sync，无代码变更）

## [v0.3.11] — 2026-11-10

- - build: 依赖地板对齐（floor sync，无代码变更）

## [v0.3.10] — 2026-11-09

- fix(async): Run 取消与真实错误并存时错误面确定（#61）
- fix(common): safeRun After-hook panic 与任务错误 Join（#62）

关联问题单：workflow#61、workflow#62。

## [v0.3.9] — 2026-11-08

- build: 依赖地板对齐——本版把 oekit 依赖地板抬到各依赖当前最新已发布版本（matrix-tools/scripts/align_floors.py 维护轮）

## [v0.3.8] — 2026-11-07

- fix(future): Wait 改可重入——结果缓存后重复/并发/超时重试调用直接返回缓存值，二次调用不再永久阻塞（#43）
- fix(async): RunAsyncWithResults 套用取消类错误豁免——口径与 Run/RunAsync 对齐（#45）
- fix(flow): Add/Case 与 Run 并发数据竞争——内部加锁 + Run 快照执行，并发契约文档化（#46）

关联问题单：workflow#43、workflow#45、workflow#46。

## [v0.3.7] — 2026-11-06

- build: 矩阵依赖地板对齐（第二轮发版后置）

## [v0.3.6] — 2026-11-06

- **BREAKING** fix(async): `RunAsync` 补取消兜底——外部 cancel/超时后 `Future.Wait` 不再假成功（[#35](https://github.com/oekit/workflow/issues/35)）——`Run` 完成路径有取消兜底（全部任务走 skip/豁免 ⇒ firstErr 为 nil ⇒ 返回 ctx.Err()，「假成功」已修），`RunAsync` 完成路径缺同一段：外部取消后直接 `future.ch <- firstErr`（nil）⇒ Wait 返回 nil ⇒ 调用方按「全部成功」继续走。修法：与 Run 同款兜底，以 `Future.canceled` 标记区分两种取消——**Future.Cancel() 显式取消保持「放弃等待即成功返回」**（TestAsyncFlow_Cancel 钉住的既有语义不变），**parent ctx 取消/超时则上抛 context 错误**（0.x minor 允许 breaking：此前依赖「外部取消返回 nil」的消费方需改判据 `err != nil && !errors.Is(err, context.Canceled)`）

关联问题单：workflow#35。

## [v0.3.5] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [v0.3.4] — 2026-11-02

- build: 矩阵内依赖地板 `concurrency` v0.23.2 → v0.23.3（维护轮全量对齐；本版不含 API 变更）

## [v0.3.3] — 2026-10-29

- build: 依赖地板对齐——concurrency 抬到 v0.23.2

## [v0.3.2] — 2026-10-19

- build: 矩阵内依赖地板对齐（align_floors 不动点）：concurrency v0.5.4 → v0.17.0、logx v0.6.0 → v0.7.0

## [v0.3.1] — 2026-10-17

- fix(deps): logx 地板 v0.5.0 → v0.6.0（零代码改动）

## [v0.3.0] — 2026-10-17

### Breaking
- **包提升至模块根**：`github.com/oekit/workflow/workflow` → `github.com/oekit/workflow`
  （仅移动目录与导入路径，代码零逻辑变化；矩阵内无消费方）


## [v0.2.7] — 2026-10-17

- concurrency 地板 v0.5.2 → v0.5.4

## [v0.2.6] — 2026-10-17

- fix(deps): 内部依赖地板 concurrency v0.1.5 → v0.5.2、logx v0.1.9 → v0.5.0；go 最低版本降至 1.24.0（自然落点）

## [v0.1.0] — 2026-10-15

自 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来的初始公开版本，完成拆分稳定化：模块路径统一为 `github.com/oekit/workflow`，多轮拆分/合并收敛至当前模块边界，组织基建（共享 CI workflow、golangci v2、Dependabot）就位。