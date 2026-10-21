# DeepRespond IM Go SDK

深应智能 IM（DeepRespond IM）的 Go 客户端 SDK：登录与令牌刷新、REST API、WebSocket 长连接与自动重连、事件订阅、登录后同步与内存 Store，全部封装在一个无第三方依赖的包里（仅标准库）。模块名 `github.com/DeepRespond-AI/deeprespond-im-go-sdk`，包名 `im`。

## 安装

```bash
go get github.com/DeepRespond-AI/deeprespond-im-go-sdk@v1.0.0-alpha
```

要求 Go 1.26 及以上，无任何第三方依赖。

## 快速上手

```go
package main

import (
    "context"
    "fmt"

    im "github.com/DeepRespond-AI/deeprespond-im-go-sdk"
)

func main() {
    ctx := context.Background()
    c, err := im.New(im.Config{
        AppKey: "org#myapp",            // 组织#应用
        APIURL: "http://127.0.0.1:18480",      // IM 服务端 REST 地址
    })
    if err != nil {
        panic(err)
    }
    if _, err := c.Login(ctx, "password", "alice", "pass123"); err != nil {
        panic(err) // 令牌自动保存到 Config.TokenStorage（缺省进程内内存）
    }
    _ = c.OnEvent(func(e im.Event) { // 长连接事件（见下文）
        // switch e.Name { case im.EventMessageReceived: ... }
    })
    if err := c.ConnectWait(ctx); err != nil { // 建立 WS 长连接并等待 auth 完成
        panic(err)
    }
    m, err := c.SendText(ctx, im.ToUser("bob"), "你好", nil)
    fmt.Println(m.Seq, err)
}
```

要点：

- `New` 不做任何网络请求；`Login(grantType, username, secret)` 目前支持 `"password"`，成功后令牌由 SDK 自动刷新。
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
| `TokenStorage` | 令牌持久化接口（`Load`/`Save`/`Clear`），实现后重启免登录 |
| `Logger` | 日志接口，缺省静默 |
| `HTTPTimeout` | REST 超时，缺省 15s |

## Demo

`demo/` 是单二进制演示程序，`go:embed` 内嵌 `web/`（`index.html` + `app.js` + `app.css`，无前端构建步骤），通过 `wsbridge.go` 把页面 WebSocket 协议映射为 SDK 调用，事件以 push（`bootstrap`/`convs`/`msgs`/`typing`/`kicked` 等）下发给页面。

```bash
go build -o /tmp/godemo ./demo
/tmp/godemo --addr :18290 --app-key 'org#myapp'   # 也可用环境变量 APP_KEY
# 打开 http://127.0.0.1:18290 ，用该应用下的账号密码登录
```

覆盖功能：密码登录与错误提示、查找用户与好友申请、单聊/群聊收发（文本、图片、引用回复）、已读回执、正在输入、消息回应（👍/❤️）、撤回与删除、草稿、置顶与免打扰标记、群创建与管理、设备管理（吊销会话被踢）、修改资料与退出登录；窄视口自适应。

`--debug` 打开 SDK 调试日志；`--api` 指向其它服务端地址。

## 测试

```bash
go test ./...    # 单元测试（store/wsframe/client，自带 httptest 桩，无需服务端）
```

## License

[MIT](LICENSE)
