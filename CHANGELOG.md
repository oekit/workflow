# Changelog — oekit/workflow

由 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来。所有 notable 变更记录如下。

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