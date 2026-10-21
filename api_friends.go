package im

// 好友关系（REST）。

import (
	"context"
	"net/http"
	"net/url"
)

// Friends 好友列表。
func (c *Client) Friends(ctx context.Context) ([]Friend, error) {
	var resp struct {
		Items []Friend `json:"items"`
	}
	err := c.getJSON(ctx, "/client/v1/friends", nil, &resp)
	return resp.Items, err
}

// CheckFriends 批量检查好友关系。
func (c *Client) CheckFriends(ctx context.Context, usernames []string) ([]FriendCheck, error) {
	var resp struct {
		Items []FriendCheck `json:"items"`
	}
	err := c.request(ctx, http.MethodPost, "/client/v1/friends/check", map[string]any{"usernames": usernames}, &resp)
	return resp.Items, err
}

// SetFriendRemark 设置好友备注与扩展属性。
func (c *Client) SetFriendRemark(ctx context.Context, username, remark string, attributes map[string]any) error {
	body := map[string]any{}
	if remark != "" {
		body["remark"] = remark
	}
	if attributes != nil {
		body["attributes"] = attributes
	}
	_, err := requestRaw[map[string]any](c, ctx, http.MethodPatch, "/client/v1/friends/"+url.PathEscape(username), body)
	return err
}

// RemoveFriend 删除好友。
func (c *Client) RemoveFriend(ctx context.Context, username string) error {
	_, err := requestRaw[map[string]any](c, ctx, http.MethodDelete, "/client/v1/friends/"+url.PathEscape(username), nil)
	return err
}

// SendFriendRequest 发好友申请。返回 status：pending 等待对方同意；
// accepted 表示对方允许任何人添加，已直接成为好友。
func (c *Client) SendFriendRequest(ctx context.Context, username, message string) (string, error) {
	resp, err := requestRaw[struct {
		Status string `json:"status"`
	}](c, ctx, http.MethodPost, "/client/v1/friend-requests",
		map[string]any{"username": username, "message": message})
	if err != nil {
		return "", err
	}
	return resp.Status, nil
}

// FriendRequests 收到/发出的好友申请列表。which: received|sent。
func (c *Client) FriendRequests(ctx context.Context, which string) ([]FriendRequest, error) {
	var p Page[FriendRequest]
	if err := c.getJSON(ctx, "/client/v1/friend-requests/"+url.PathEscape(which), nil, &p); err != nil {
		return nil, err
	}
	return p.Items, nil
}

// AcceptFriendRequest 接受好友申请。
func (c *Client) AcceptFriendRequest(ctx context.Context, username string) error {
	_, err := requestRaw[map[string]any](c, ctx, http.MethodPost,
		"/client/v1/friend-requests/received/"+url.PathEscape(username)+"/accept", map[string]any{})
	return err
}

// DeclineFriendRequest 拒绝好友申请。
func (c *Client) DeclineFriendRequest(ctx context.Context, username string) error {
	return c.request(ctx, http.MethodPost,
		"/client/v1/friend-requests/received/"+url.PathEscape(username)+"/decline", map[string]any{}, nil)
}

// MarkFriendRequestsRead 申请列表标为已读。
func (c *Client) MarkFriendRequestsRead(ctx context.Context) error {
	return c.request(ctx, http.MethodPost, "/client/v1/friend-requests/received/read", nil, nil)
}

// FriendRequestUnreadCount 申请未读数。
func (c *Client) FriendRequestUnreadCount(ctx context.Context) (int64, error) {
	var resp struct {
		UnreadCount int64 `json:"unread_count"`
	}
	err := c.getJSON(ctx, "/client/v1/friend-requests/received/unread-count", nil, &resp)
	return resp.UnreadCount, err
}

// FriendSettings 好友添加模式（anyone|request）。
func (c *Client) FriendSettings(ctx context.Context) (string, error) {
	var resp struct {
		FriendAddMode string `json:"friend_add_mode"`
	}
	err := c.getJSON(ctx, "/client/v1/friend-settings", nil, &resp)
	return resp.FriendAddMode, err
}

// SetFriendSettings 设置好友添加模式。
func (c *Client) SetFriendSettings(ctx context.Context, mode string) error {
	return c.request(ctx, http.MethodPut, "/client/v1/friend-settings", map[string]any{"friend_add_mode": mode}, nil)
}

// Blacklist 黑名单列表。
func (c *Client) Blacklist(ctx context.Context) ([]string, error) {
	var resp struct {
		Items []struct {
			Username string `json:"username"`
		} `json:"items"`
	}
	err := c.getJSON(ctx, "/client/v1/blacklist", nil, &resp)
	out := make([]string, len(resp.Items))
	for i, it := range resp.Items {
		out[i] = it.Username
	}
	return out, err
}

// BlockUser 加入黑名单。
func (c *Client) BlockUser(ctx context.Context, username string) error {
	return c.request(ctx, http.MethodPut, "/client/v1/blacklist/"+url.PathEscape(username), map[string]any{}, nil)
}

// UnblockUser 移出黑名单。
func (c *Client) UnblockUser(ctx context.Context, username string) error {
	return c.request(ctx, http.MethodDelete, "/client/v1/blacklist/"+url.PathEscape(username), nil, nil)
}

// requestRaw 泛型请求（out 为 map 时不想建模就用它）。
func requestRaw[R any](c *Client, ctx context.Context, method, path string, body any) (*R, error) {
	var out R
	err := c.request(ctx, method, path, body, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}
