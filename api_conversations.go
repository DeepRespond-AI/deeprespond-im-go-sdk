package im

// 会话操作（REST）。

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

// RecentConversations 拉取最近会话（含 cursor 翻页）。
func (c *Client) RecentConversations(ctx context.Context, cursor string, limit int64) (*Page[ConversationState], error) {
	if limit <= 0 {
		limit = 100
	}
	q := url.Values{"limit": {strconv.FormatInt(limit, 10)}}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	var p Page[ConversationState]
	err := c.getJSON(ctx, "/client/v1/conversations/recent", q, &p)
	return &p, err
}

// Conversation 获取单个会话状态。
func (c *Client) Conversation(ctx context.Context, id string) (*ConversationState, error) {
	var st ConversationState
	if err := c.request(ctx, http.MethodGet, "/client/v1/conversations/"+id, nil, &st); err != nil {
		return nil, err
	}
	c.store.PutConversation(&st)
	return &st, nil
}

// SingleConversation 获取与对端用户的单聊会话（不存在则返回 not_found）。
func (c *Client) SingleConversation(ctx context.Context, peer string) (*ConversationState, error) {
	var st ConversationState
	err := c.getJSON(ctx, "/client/v1/single-conversations/"+url.PathEscape(peer), nil, &st)
	return &st, err
}

// DeleteConversation 删除会话（隐藏并清空本地）。
func (c *Client) DeleteConversation(ctx context.Context, id string) error {
	return c.request(ctx, http.MethodDelete, "/client/v1/conversations/"+id, nil, nil)
}

// ClearConversation 清空会话历史（服务端推进 cleared_seq）。
func (c *Client) ClearConversation(ctx context.Context, id string) error {
	return c.request(ctx, http.MethodPost, fmt.Sprintf("/client/v1/conversations/%s/clear", id), nil, nil)
}

// SetConversationPinned 置顶/取消置顶会话。
func (c *Client) SetConversationPinned(ctx context.Context, id string, pinned bool) error {
	return c.request(ctx, http.MethodPut, fmt.Sprintf("/client/v1/conversations/%s/pin", id), map[string]any{"pinned": pinned}, nil)
}

// MarkConversationUnread 标记会话未读。
func (c *Client) MarkConversationUnread(ctx context.Context, id string, marked bool) error {
	return c.request(ctx, http.MethodPut, fmt.Sprintf("/client/v1/conversations/%s/unread-mark", id), map[string]any{"marked": marked}, nil)
}

// SetConversationDraft 设置会话草稿（本地即可，服务端仅做透传存储）。
func (c *Client) SetConversationDraft(ctx context.Context, id, draft string) error {
	return c.request(ctx, http.MethodPut, fmt.Sprintf("/client/v1/conversations/%s/draft", id), map[string]any{"draft": draft}, nil)
}

// ReadAllConversations 全部会话标记已读。
func (c *Client) ReadAllConversations(ctx context.Context) error {
	return c.request(ctx, http.MethodPost, "/client/v1/conversations/read-all", nil, nil)
}

// UnreadCount 总未读数。
func (c *Client) UnreadCount(ctx context.Context) (int64, error) {
	var resp struct {
		Total int64 `json:"total"`
	}
	err := c.getJSON(ctx, "/client/v1/conversations/unread-count", nil, &resp)
	return resp.Total, err
}

// GroupReceipts 群聊已读回执（单聊无此接口，只有 state.peer_read_seq）。
func (c *Client) GroupReceipts(ctx context.Context, conversationID string, seqs []int64) error {
	return c.request(ctx, http.MethodPost, fmt.Sprintf("/client/v1/conversations/%s/receipts", conversationID), map[string]any{"seqs": seqs}, nil)
}

// ReactionUsers 回应某 key 的用户列表。
func (c *Client) ReactionUsers(ctx context.Context, conversationID string, seq int64, key string) (*Page[string], error) {
	var p Page[string]
	err := c.getJSON(ctx, fmt.Sprintf("/client/v1/conversations/%s/messages/%d/reactions/%s/users", conversationID, seq, url.PathEscape(key)), nil, &p)
	return &p, err
}
