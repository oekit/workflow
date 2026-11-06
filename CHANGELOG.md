# Changelog — oekit/workflow

由 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来。所有 notable 变更记录如下。

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