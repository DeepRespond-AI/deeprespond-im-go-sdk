# DeepRespond IM Go 客户端 SDK

当前版本：**v1.0.1**。仅依赖 Go 标准库，需要 **Go 1.26.5 或以上**。

以一个 IM 用户的身份运行，提供登录与令牌刷新、REST API、WebSocket 长连接与自动重连、事件订阅、同步引擎及内存 Store，适用于机器人、客服账号和常驻客户端。
应用管理、服务端凭证和签名回调请使用 [Go 服务端 SDK](https://www.syznlink.com/docs/server-sdk/go/)。

## 安装

```bash
go get github.com/DeepRespond-AI/deeprespond-im-go-sdk@v1.0.1
```

模块路径为 `github.com/DeepRespond-AI/deeprespond-im-go-sdk`，包名为 `im`。
源码和 Demo 可从 [v1.0.1 标签](https://github.com/DeepRespond-AI/deeprespond-im-go-sdk/tree/v1.0.1) 获取；从仓库根目录运行 `go run ./demo`。

## v1.0.1 更新

- 同步最新客户端 SDK 和 Web Demo 源码及使用说明。
- 修复批量群成员变更结果解析，支持 `status`、`code`、`message`、`details`，保留旧 `result`、`error` 字段以兼容历史返回。
- 补充群成员结果单元测试和可选的真实服务集成测试源码。
- 默认登录与长连接上报版本更新为 `1.0.1`；自定义 `Config.SDKVersion` 继续生效。

完整记录见 [CHANGELOG.md](CHANGELOG.md)。

## 快速上手

```go
package main

import (
    "context"
    "fmt"
    "os"
    "time"

    im "github.com/DeepRespond-AI/deeprespond-im-go-sdk"
)

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    c, err := im.New(im.Config{
        AppKey: os.Getenv("IM_APP_KEY"),
        APIURL: os.Getenv("IM_API_URL"),
    })
    if err != nil {
        panic(err)
    }
    if _, err := c.Login(ctx, im.LoginTicket, os.Getenv("IM_USERNAME"), os.Getenv("IM_TICKET")); err != nil {
        panic(err)
    }
    defer func() {
        logoutCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
        defer stop()
        _ = c.Logout(logoutCtx)
    }()
    c.OnEvent(func(e im.Event) {
        fmt.Println("事件：", e.Name)
    })
    if err := c.ConnectWait(ctx); err != nil { // 建立 WS 长连接并等待 auth 完成
        panic(err)
    }
    m, err := c.SendText(ctx, im.ToUser(os.Getenv("IM_TO_USER")), "你好", nil)
    if err != nil {
        panic(err)
    }
    fmt.Println("已发送，序号：", m.Seq)
}
```

要点：

- `New` 不做任何网络请求；`Login` 支持 `im.LoginTicket` 和 `im.LoginPassword`。正式接入由业务服务端验证用户身份并签发票据；密码登录用于开发测试。
- 运行示例前设置 `IM_API_URL`、`IM_APP_KEY`、`IM_USERNAME`、`IM_TICKET` 和 `IM_TO_USER`。票据只使用一次；再次运行需要重新签发。此示例发送后退出，持续收消息的程序见官网快速集成。
- REST 方法在未连接时也可直接调用；`Connect` / `ConnectWait` 建立长连接后才能收事件、收推送和使用 WS 通道（如 `Typing`）。
- 一条长连接对应一个登录用户，多账号创建多个 `Client` 实例即可。

## 能力总览

| 分组 | 方法（`Client` 上） |
| --- | --- |
| 用户 | `Me` `UpdateProfile` `SetPassword` `GetUser` `LookupUsers` `QueryPresence` `SubscribePresence` `UnsubscribePresence` `Sessions` `RevokeSession` `ClientConfig` `Report` |
| 好友与黑名单 | `Friends` `CheckFriends` `SetFriendRemark` `RemoveFriend` `SendFriendRequest` `FriendRequests` `AcceptFriendRequest` `DeclineFriendRequest` `MarkFriendRequestsRead` `FriendRequestUnreadCount` `FriendSettings` `SetFriendSettings` `Blacklist` `BlockUser` `UnblockUser` |
| 群组 | `CreateGroup` `Group` `LookupGroups` `MyGroups` `PublicGroups` `UpdateGroup` `DismissGroup` `LeaveGroup` `GroupMembers` `AddGroupMembers` `RemoveGroupMember` `SetGroupMemberRole` `GroupInvitations` `AcceptGroupInvitation` `DeclineGroupInvitation` `MyGroupApplications` |
| 会话 | `RecentConversations` `Conversation` `SingleConversation` `DeleteConversation` `ClearConversation` `SetConversationPinned` `MarkConversationUnread` `SetConversationDraft` `ReadAllConversations` `UnreadCount` `GroupReceipts` `ReactionUsers` |
| 消息 | `SendText` `Send` `SendMedia` `SendReply` `SendWithMentions` `Recall` `DeleteMessages` `AddReaction` `RemoveReaction` `PinMessage` `UnpinMessage` `MessageLookup` `Read` `Typing` |
| 媒体 | `Upload`（预签名直传）`DownloadURLs` `Download` |
| 长连接 | `Connect` `ConnectWait` `Disconnect` `State` `OnState` `OnEvent` `WSCall` |

消息发送统一使用 `Target`（`im.ToUser(username)` / `im.ToGroup(groupID)`）；图片、文件、语音、视频等先 `Upload` 得到 `FileInfo`，再 `SendMedia` 落消息。

## 事件与同步

- `OnEvent` 订阅服务端推送，事件常量覆盖 `message.*`（received/changed/deleted/online/conversation_changed/peer_read/read_by/typing/sync_hint）、`friend.*`、`blacklist.changed`、`group.*`、`user.*`、`presence.changed`。
- 登录后 `SyncAll` 执行全量同步（recent → group-digest → 增量 sync → last-messages 补最后一条），此后增量事件由 SDK 自动应用。
- `Store`（`c.Store()`）是线程安全的内存缓存：会话状态、消息（按 seq 有序应用，乱序自动补齐，`cleared_seq` 变大自动清理）、群与成员、好友等，可直接读也可自行加锁遍历。
- 服务端对发送者不回显 `message.received`、新会话不向发送者推会话事件，SDK 发送成功后会在本地兜底建立/推进会话状态（`upsertSentConv`）。

## 可选配置

| 字段 | 说明 |
| --- | --- |
| `WSURL` | 缺省由 `APIURL` 推导（http→ws + `/client/v1/ws`） |
| `DeviceID` / `DeviceName` / `Platform` | 设备标识，缺省自动生成；影响「多设备与设备管理」 |
| `TokenStorage` | 可选令牌持久化接口（`Load`/`Save`/`Clear`）；默认不跨进程保存令牌，恢复时需同时固定 `DeviceID` |
| `Logger` | 日志接口，缺省静默 |
| `HTTPTimeout` | REST 超时，缺省 15s |

## Demo

单二进制演示程序，`go:embed` 内嵌 `web/`（`index.html` + `app.js` + `app.css`，无前端构建步骤），通过 `wsbridge.go` 把页面 WebSocket 协议映射为 SDK 调用，事件以 push（`bootstrap`/`convs`/`msgs`/`typing`/`kicked` 等）下发给页面。

```bash
go run ./demo --addr 127.0.0.1:8090 \
  --api http://127.0.0.1:18480 --app-key '组织#应用'
# 打开 http://127.0.0.1:8090，用该应用下的账号密码登录。
```

覆盖功能：密码登录与错误提示、查找用户与好友申请、单聊/群聊收发（文本、图片、引用回复）、已读回执、正在输入、消息回应（👍/❤️）、撤回与删除、草稿、置顶与免打扰标记、群创建与管理、设备管理（吊销会话被踢）、修改资料与退出登录；窄视口自适应。

`--debug` 打开 SDK 调试日志；`--api` 指向其它服务端地址。
参数、双账号测试、源码构建与排查步骤见 [Demo 指南](demo/README.md)。客户端 Demo 不需要 Client Secret 或签名回调地址。

## 测试

```bash
go test ./...                 # 单元测试（store/wsframe/client/types）
# 集成测试：连真实服务端（默认 127.0.0.1:18480/18490，可用环境变量覆盖）
IM_API=http://127.0.0.1:18480 IM_CTRL=http://127.0.0.1:18490 \
IM_APPKEY='6526900072#myapp' go test -tags integration -v ./...
```

- `IM_API`：服务端 REST+WS 地址；`IM_CTRL`：harness 控制口（创建测试用户等）；`IM_APPKEY`：被测应用。
- harness 启动与用法见完整仓库的 [sdk/conformance](https://gitlab.deeprespond.cn/normal/im/-/blob/develop/sdk/conformance/README.md)。压缩包中的 `go test ./...` 可直接运行，集成测试需要完整仓库与真实 IM 服务。
- 普通 `go test ./...` 不依赖真实服务；带 `integration` 标签的测试需要另外准备 IM 服务和 harness，不能把跳过测试视为集成验收通过。
