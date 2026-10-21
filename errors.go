// Package im 是 DeepRespond IM 的 Go 客户端 SDK：REST + WebSocket 长连接，
// 覆盖《docs/消息/3.接口设计》《docs/网关/3.接口设计》定义的客户端功能。
//
// 用法概览：
//
//	cli, err := im.New(im.Config{AppKey: "org#app", APIURL: "https://im.example.com"})
//	session, err := cli.Login(im.LoginPassword, "alice", "pass-123")
//	cli.OnEvent(func(e im.Event) { ... })
//	cli.Start()
//	msg, err := cli.SendText(im.ToUser("bob"), "你好")
package im

import (
	"encoding/json"
	"fmt"
	"strings"
)

// APIError 是服务端返回的业务错误（HTTP 非 2xx 或 WS reply.error）。
type APIError struct {
	Code       string          `json:"code"`                 // 机器可读，如 unauthenticated、not_found
	Message    string          `json:"message,omitempty"`    // 人类可读描述
	Details    json.RawMessage `json:"details,omitempty"`    // 附加细节，如 {"reason":"token_expired"}
	RequestID  string          `json:"request_id,omitempty"` // 服务端排障 ID
	HTTPStatus int             `json:"-"`                    // REST 时的 HTTP 状态码；WS 时为 0
}

func (e *APIError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "im: %s", e.Code)
	if e.Message != "" {
		fmt.Fprintf(&b, ": %s", e.Message)
	}
	if len(e.Details) > 0 {
		fmt.Fprintf(&b, " (details %s)", e.Details)
	}
	if e.RequestID != "" {
		fmt.Fprintf(&b, " [request_id %s]", e.RequestID)
	}
	return b.String()
}

// Reason 返回 details.reason（如 4001 关闭帧的 auth_timeout、续期失败的 expired）。
func (e *APIError) Reason() string {
	var d struct {
		Reason string `json:"reason"`
	}
	if e.Details != nil && json.Unmarshal(e.Details, &d) == nil {
		return d.Reason
	}
	return ""
}

// IsCode 报告 err 是否为给定错误码的服务端错误。
func IsCode(err error, code string) bool {
	apiErr, ok := err.(*APIError)
	return ok && apiErr.Code == code
}

// 常见错误码。
const (
	CodeUnauthenticated = "unauthenticated"
	CodeForbidden       = "forbidden"
	CodeNotFound        = "not_found"
	CodeInvalidArgument = "invalid_argument"
	CodeVersionConflict = "version_conflict"
	CodeRateLimited     = "rate_limited"
	CodePermission      = "permission_denied"
	CodeInternal        = "internal"
	CodeUnavailable     = "unavailable"
)

// SessionValid 报告当前是否持有未过期的访问令牌。
func (c *Client) SessionValid() bool {
	return c.token() != ""
}
