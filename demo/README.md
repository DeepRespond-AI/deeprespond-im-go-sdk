# Go 客户端 SDK Demo

浏览器聊天示例：每个浏览器会话持有一个 `im.Client`，由 SDK 登录用户、建立长连接、同步消息并收发事件。
Go 内嵌 `web/`，无需安装 Node.js 或构建前端。

## 准备与启动

- Go **1.26.5 或以上**，与 `go.mod` 一致。
- 可访问的 IM 服务地址、AppKey（`组织#应用`），以及同一应用下两个用户的密码。
- 使用客户端接口，不需要 Client ID、Client Secret 或签名回调地址。应用管理与签名回调示例见 [服务端 Demo](https://www.syznlink.com/docs/server-sdk/go/demo)。

在本仓库根目录执行：

```bash
go run ./demo \
  --api http://127.0.0.1:18480 \
  --app-key '组织#应用' \
  --addr 127.0.0.1:8090
```

打开 `http://127.0.0.1:8090`，登录一个账号；再用另一个浏览器或无痕窗口登录第二个账号，进行单聊和群聊验证。
同一浏览器的标签页共享 Cookie，不能当成两个独立账号。

## 下载包与二进制构建

[下载 v1.0.2 源码 ZIP](https://github.com/DeepRespond-AI/deeprespond-im-go-sdk/archive/refs/tags/v1.0.2.zip)。

解压后进入 `deeprespond-im-go-sdk-1.0.2`。SDK 与页面源码已包含在仓库中，在仓库根目录编译：

```bash
go build -o /tmp/im-go-client-demo ./demo
/tmp/im-go-client-demo --api http://127.0.0.1:18480 \
  --app-key '组织#应用' --addr 127.0.0.1:8090
```

Linux/macOS 示例使用 `/tmp`；Windows 可改为 `go build -o im-go-client-demo.exe ./demo`。

## 参数与验证

| 参数 | 默认值或环境变量 | 作用 |
| --- | --- | --- |
| `--api` | `http://127.0.0.1:18480` | Demo 后端连接的 IM 服务地址 |
| `--app-key` | `APP_KEY` | 应用标识，必须与登录账号所属应用一致 |
| `--addr` | `:8090` | HTTP 监听地址；本机体验建议显式使用 `127.0.0.1:8090` |
| `--debug` | 关闭 | 输出 SDK 调试日志 |

进程开始监听后，依次验证：登录、好友申请、收发文本与图片、历史记录、已读、群聊与退出登录。
Demo 还展示引用回复、回应、撤回与删除、草稿、置顶、群管理、资料及设备管理，布局适配窄视口。

## 结构与排查

| 文件 | 职责 |
| --- | --- |
| `main.go` | HTTP 路由、浏览器会话与 SDK 调用 |
| `wsbridge.go` | 页面 WebSocket 桥 |
| `web/index.html`、`app.js`、`app.css` | 页面结构、交互和样式，通过 `go:embed` 内嵌 |

浏览器只连接 Demo；Demo 后端通过 REST / WebSocket 连接 IM。
因此 `--api` 必须能从运行 Go 程序的机器访问，浏览器不直接持有用户令牌，也不直接访问该 IM 地址。

| 现象 | 检查 |
| --- | --- |
| 启动提示缺少 AppKey | 设置 `--app-key` 或 `APP_KEY`，格式包含 `#` |
| 登录失败 | 账号是否存在于该应用、密码与应用密码登录策略是否正确 |
| 无法收到对方消息 | 两个独立浏览器会话是否都完成登录和连接，IM 长连接入口是否可达 |
| 地址被占用 | 修改 `--addr`，访问对应端口 |
| 找不到 SDK 模块 | 从解压后的仓库根目录运行 Demo；业务工程检查模块安装与导入路径 |

此 Demo 与服务端 Demo 都使用 `demo_sid` Cookie。本机同时体验时请使用不同浏览器上下文，避免互相覆盖登录。
用户令牌和内存缓存随进程结束而丢失。常驻业务程序需要令牌持久化时，参考 [SDK 配置](../README.md#可选配置)。

单元与真实服务测试命令见 [SDK 测试说明](../README.md#测试)，服务端测试环境见 [harness](https://gitlab.deeprespond.cn/normal/im/-/blob/develop/sdk/conformance/README.md)。
