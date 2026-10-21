package im

// 用户、资料、在线状态、设备、配置、上报。

import (
	"context"
	"net/http"
	"net/url"
)

// Me 获取本人资料。
func (c *Client) Me(ctx context.Context) (*SelfProfile, error) {
	var p SelfProfile
	err := c.request(ctx, http.MethodGet, "/client/v1/me", nil, &p)
	return &p, err
}

// UpdateProfile 更新本人资料（nickname/avatar_url/attributes）。
func (c *Client) UpdateProfile(ctx context.Context, nickname, avatarURL string, attributes map[string]any) (*SelfProfile, error) {
	body := map[string]any{}
	if nickname != "" {
		body["nickname"] = nickname
	}
	if avatarURL != "" {
		body["avatar_url"] = avatarURL
	}
	if attributes != nil {
		body["attributes"] = attributes
	}
	var p SelfProfile
	err := c.request(ctx, http.MethodPatch, "/client/v1/me", body, &p)
	return &p, err
}

// SetPassword 设置/修改密码（oldPassword 为空表示首次设置）。
func (c *Client) SetPassword(ctx context.Context, oldPassword, newPassword string) error {
	body := map[string]any{"password": newPassword}
	if oldPassword != "" {
		body["old_password"] = oldPassword
	}
	return c.request(ctx, http.MethodPost, "/client/v1/me/password", body, nil)
}

// GetUser 获取指定用户公开资料。
func (c *Client) GetUser(ctx context.Context, username string) (*UserProfile, error) {
	var p UserProfile
	err := c.getJSON(ctx, "/client/v1/users/"+url.PathEscape(username), nil, &p)
	return &p, err
}

// LookupUsers 批量查询用户（≤100）。
func (c *Client) LookupUsers(ctx context.Context, usernames []string) ([]UserProfile, error) {
	var resp struct {
		Items []UserProfile `json:"items"`
	}
	err := c.request(ctx, http.MethodPost, "/client/v1/users/lookup", map[string]any{"usernames": usernames}, &resp)
	return resp.Items, err
}

// QueryPresence 查询在线状态。
func (c *Client) QueryPresence(ctx context.Context, usernames []string) ([]PresenceItem, error) {
	var resp struct {
		Items []PresenceItem `json:"items"`
	}
	err := c.request(ctx, http.MethodPost, "/client/v1/presence/query", map[string]any{"usernames": usernames}, &resp)
	return resp.Items, err
}

// SubscribePresence 订阅在线状态（应用开启在线状态功能时可用）。
func (c *Client) SubscribePresence(ctx context.Context, usernames []string) error {
	_, err := c.WSCall(ctx, "presence.subscribe", map[string]any{"usernames": usernames})
	return err
}

// UnsubscribePresence 取消订阅。
func (c *Client) UnsubscribePresence(ctx context.Context, usernames []string) error {
	data := map[string]any{"usernames": usernames}
	if len(usernames) == 0 {
		data = map[string]any{"all": true}
	}
	_, err := c.WSCall(ctx, "presence.unsubscribe", data)
	return err
}

// Sessions 我的登录会话（多端设备）。
func (c *Client) Sessions(ctx context.Context) ([]LoginSession, error) {
	var resp struct {
		Items []LoginSession `json:"items"`
	}
	err := c.getJSON(ctx, "/client/v1/me/sessions", nil, &resp)
	return resp.Items, err
}

// RevokeSession 踢掉指定登录会话。
func (c *Client) RevokeSession(ctx context.Context, sessionID string) error {
	return c.request(ctx, http.MethodDelete, "/client/v1/me/sessions/"+url.PathEscape(sessionID), nil, nil)
}

// ClientConfig 应用级配置。
func (c *Client) ClientConfig(ctx context.Context) (map[string]any, error) {
	var cfg map[string]any
	err := c.getJSON(ctx, "/client/v1/config", nil, &cfg)
	return cfg, err
}

// Report 上报（崩溃/日志/统计等，reason ≤256 字符）。
func (c *Client) Report(ctx context.Context, reason string, attributes map[string]any) error {
	body := map[string]any{"reason": reason}
	if attributes != nil {
		body["attributes"] = attributes
	}
	return c.request(ctx, http.MethodPost, "/client/v1/reports", body, nil)
}
