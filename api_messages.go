package im

// 消息发送与操作。优先走 WS（在线低延迟），连接不可用时自动回退 REST。

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// SendText 发送文本消息。
func (c *Client) SendText(ctx context.Context, target Target, text string, ext map[string]any) (*Message, error) {
	return c.send(ctx, target, "text", mustJSON(map[string]string{"text": text}), ext, nil, 0, false, false, false)
}

// Send 发送自定义消息（完整 body 由调用方给出）。
func (c *Client) Send(ctx context.Context, target Target, msgType string, body []byte, ext map[string]any, onlineOnly bool) (*Message, error) {
	return c.send(ctx, target, msgType, body, ext, nil, 0, false, false, onlineOnly)
}

// SendMedia 发送媒体消息（body 为上传返回的 FileInfo JSON）。
func (c *Client) SendMedia(ctx context.Context, target Target, msgType string, info *FileInfo, ext map[string]any) (*Message, error) {
	return c.send(ctx, target, msgType, mustJSON(info), ext, nil, 0, false, false, false)
}

// SendReply 回复指定 seq 的消息。
func (c *Client) SendReply(ctx context.Context, target Target, replySeq int64, msgType string, body []byte, ext map[string]any) (*Message, error) {
	return c.send(ctx, target, msgType, body, ext, nil, replySeq, false, false, false)
}

// SendWithMentions 发送带提醒的消息（usernames 与 all 恰选其一）。
func (c *Client) SendWithMentions(ctx context.Context, target Target, msgType string, body []byte, usernames []string, all bool, ext map[string]any) (*Message, error) {
	return c.send(ctx, target, msgType, body, ext, &Mentions{Usernames: usernames, All: all}, 0, false, false, false)
}

// send 统一发送入口：WS 可用走 message.send，否则 REST POST /messages。
func (c *Client) send(ctx context.Context, target Target, msgType string, body []byte, ext map[string]any, mentions *Mentions, replyTo int64, needReceipt, excludeUnread, onlineOnly bool) (*Message, error) {
	if msgType == "" {
		msgType = "text"
	}
	if body == nil {
		body = mustJSON(map[string]string{})
	}
	in := map[string]any{
		"client_msg_id": newClientMsgID(),
		"type":          msgType,
		// 必须用 RawMessage：[]byte 会被 encoding/json 编码成 base64 字符串
		"body": json.RawMessage(body),
	}
	switch target.kind {
	case "user":
		in["to_user"] = target.id
	case "group":
		in["to_group"] = target.id
	default:
		return nil, fmt.Errorf("im: 发送目标必须为 ToUser 或 ToGroup")
	}
	if ext != nil {
		in["ext"] = ext
	}
	if mentions != nil {
		in["mentions"] = mentions
	}
	if replyTo > 0 {
		in["reply_to"] = map[string]any{"seq": replyTo}
	}
	if needReceipt {
		in["need_receipt"] = true
	}
	if excludeUnread {
		in["exclude_from_unread"] = true
	}

	// WS 优先
	if c.State() == StateConnected {
		data, err := c.WSCall(ctx, "message.send", in)
		if err == nil {
			var m Message
			if jsonUnmarshal(data, &m) == nil {
				c.store.PutMessage(&m)
				c.store.upsertSentConv(&m, target)
				return &m, nil
			}
		} else if IsCode(err, "unavailable") || IsCode(err, "timeout") {
			// 连接刚好断开：回退 REST
		} else {
			return nil, err // 业务错误直接返回
		}
	}

	if onlineOnly {
		in["online_only"] = true
	}
	var m Message
	if err := c.request(ctx, http.MethodPost, "/client/v1/messages", in, &m); err != nil {
		return nil, err
	}
	c.store.PutMessage(&m)
	c.store.upsertSentConv(&m, target)
	return &m, nil
}

// Recall 撤回消息。
func (c *Client) Recall(ctx context.Context, conversationID string, seq int64) error {
	var resp struct {
		Changed bool `json:"changed"`
	}
	return c.request(ctx, http.MethodPost, fmt.Sprintf("/client/v1/conversations/%s/messages/%d/recall", conversationID, seq), nil, &resp)
}

// DeleteMessages 删除消息（本人视角）。
func (c *Client) DeleteMessages(ctx context.Context, conversationID string, seqs []int64) error {
	return c.request(ctx, http.MethodPost, fmt.Sprintf("/client/v1/conversations/%s/messages/delete", conversationID), map[string]any{"seqs": seqs}, nil)
}

// AddReaction / RemoveReaction 消息回应。
func (c *Client) AddReaction(ctx context.Context, conversationID string, seq int64, key string) error {
	return c.request(ctx, http.MethodPut,
		fmt.Sprintf("/client/v1/conversations/%s/messages/%d/reactions/%s", conversationID, seq, key), nil, nil)
}
func (c *Client) RemoveReaction(ctx context.Context, conversationID string, seq int64, key string) error {
	return c.request(ctx, http.MethodDelete,
		fmt.Sprintf("/client/v1/conversations/%s/messages/%d/reactions/%s", conversationID, seq, key), nil, nil)
}

// PinMessage / UnpinMessage 置顶消息。
func (c *Client) PinMessage(ctx context.Context, conversationID string, seq int64) error {
	return c.request(ctx, http.MethodPut, fmt.Sprintf("/client/v1/conversations/%s/messages/%d/pin", conversationID, seq), nil, nil)
}
func (c *Client) UnpinMessage(ctx context.Context, conversationID string, seq int64) error {
	return c.request(ctx, http.MethodDelete, fmt.Sprintf("/client/v1/conversations/%s/messages/%d/pin", conversationID, seq), nil, nil)
}

// MessageLookup 按 seq 批量取消息（≤100）。
func (c *Client) MessageLookup(ctx context.Context, conversationID string, seqs []int64) ([]*Message, error) {
	var resp struct {
		Items []*Message `json:"items"`
	}
	err := c.request(ctx, http.MethodPost, fmt.Sprintf("/client/v1/conversations/%s/messages/lookup", conversationID), map[string]any{"seqs": seqs}, &resp)
	return resp.Items, err
}

// Read 标记会话已读（WS 优先）。
func (c *Client) Read(ctx context.Context, conversationID string, seq int64) error {
	if c.State() == StateConnected {
		data := map[string]any{"conversation_id": conversationID}
		if seq > 0 {
			data["seq"] = seq
		}
		if _, err := c.WSCall(ctx, "message.read", data); err == nil {
			c.store.mu.Lock()
			if st := c.store.convs[conversationID]; st != nil {
				target := seq
				if target == 0 {
					target = st.MaxSeq
				}
				if target > st.ReadSeq {
					st.ReadSeq = target
					if st.UnreadCount > 0 {
						st.UnreadCount = st.MaxSeq - target
						if st.UnreadCount < 0 {
							st.UnreadCount = 0
						}
					}
				}
			}
			c.store.mu.Unlock()
			return nil
		}
	}
	body := map[string]any{}
	if seq > 0 {
		body["seq"] = seq
	}
	return c.request(ctx, http.MethodPost, fmt.Sprintf("/client/v1/conversations/%s/read", conversationID), body, nil)
}

// Typing 发送输入中通知（WS 优先，失败静默）。
func (c *Client) Typing(ctx context.Context, conversationID string) {
	if c.State() == StateConnected {
		_, _ = c.WSCall(ctx, "message.typing", map[string]any{"conversation_id": conversationID})
	}
}

// mustJSON 序列化，失败 panic（调用方传入的必须是可序列化值）。
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Errorf("im: %w", err))
	}
	return b
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
