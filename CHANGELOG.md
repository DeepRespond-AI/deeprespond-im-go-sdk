# 更新记录

## v1.0.1 — 2026-10-29

- 同步 IM 主仓库 `7b77a101` 中的 Go 客户端 SDK 与 Web Demo；SDK 最近业务修复来源为 `6ba1bf1d`。
- `GroupMemberResult` 新增 `Status`、`Code`、`Message`、`Details`，正确保留每位群成员的邀请、入群、审批和失败结果。
- 保留 `Result` 与 `Error` 字段，兼容旧返回。
- 同步群成员结果回归测试和可选集成测试源码，更新独立模块的 SDK / Demo 文档。
- 默认 `Config.SDKVersion` 从 `1.0.0-alpha` 更新为 `1.0.1`，调用方显式设置的版本不受影响。
- 模块路径保持 `github.com/DeepRespond-AI/deeprespond-im-go-sdk`，最低 Go 版本为 1.26.5。

## v1.0.0-alpha — 2026-10-21

- 首次发布 Go 客户端 SDK：REST、WebSocket、同步引擎、内存 Store 与零第三方依赖 Web Demo。
