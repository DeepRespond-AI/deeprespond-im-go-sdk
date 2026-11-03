# 更新记录

## v1.0.2 — 2026-11-03

- 新增 AI 回复（`ai.go`）：`Client.AI()` 返回 `AIModule`，按长连接上的 `ai.delta`、`ai.snapshot` 累积生成中的内容，断线重连后从 `/client/v1/ai/streams/{id}/deltas` 补齐；`State`、`OnAI`、`Stop`、`Get`，包级函数 `BodyOf`。
- 同步 IM 主仓库 `fd59ccb3` 中的 Go 客户端 SDK 与 Web Demo（AI 回复的显示与停止生成）。
- 默认 `Config.SDKVersion` 更新为 `1.0.2`。

## v1.0.1 — 2026-10-29

- 同步 IM 主仓库 `7b77a101` 中的 Go 客户端 SDK 与 Web Demo；SDK 最近业务修复来源为 `6ba1bf1d`。
- `GroupMemberResult` 新增 `Status`、`Code`、`Message`、`Details`，正确保留每位群成员的邀请、入群、审批和失败结果。
- 保留 `Result` 与 `Error` 字段，兼容旧返回。
- 同步群成员结果回归测试和可选集成测试源码，更新独立模块的 SDK / Demo 文档。
- 默认 `Config.SDKVersion` 从 `1.0.0-alpha` 更新为 `1.0.1`，调用方显式设置的版本不受影响。
- 模块路径保持 `github.com/DeepRespond-AI/deeprespond-im-go-sdk`，最低 Go 版本为 1.26.5。

## v1.0.0-alpha — 2026-10-21

- 首次发布 Go 客户端 SDK：REST、WebSocket、同步引擎、内存 Store 与零第三方依赖 Web Demo。
