package im

// 登录后同步（《消息 3.接口设计》第 6.7 节）：全量 sync → recent 兜底 →
// group-digest → last-messages 补最后一条；轻量同步按 changes 增量。

import (
	"context"
	"net/url"
	"strconv"
	"time"
)

// recentEntry 是 conversations/recent 的一项：会话状态连同最后一条消息。
type recentEntry struct {
	Conversation ConversationState `json:"conversation"`
	LastMessage  *Message          `json:"last_message"`
}

// SyncAll 登录后的全量同步（《接口设计》第 7.3 节）：recent 会话列表 →
// group-digest 群摘要 → 增量 sync → last-messages 补最后一条。写入 Store 并返回同步的会话数。
func (c *Client) SyncAll(ctx context.Context) (int, error) {
	total, version, epoch, err := c.syncRecent(ctx)
	if err != nil {
		return 0, err
	}

	// 2) group-digest：全部群会话的状态摘要（空页也可能有，按 cursor 翻完）
	dCursor := ""
	for {
		q := url.Values{"limit": {"1000"}}
		if dCursor != "" {
			q.Set("cursor", dCursor)
		}
		var resp struct {
			Items      []ConversationState `json:"items"`
			NextCursor *string             `json:"next_cursor"`
		}
		if err := c.getJSON(ctx, "/client/v1/conversations/group-digest", q, &resp); err != nil {
			return total, err
		}
		for i := range resp.Items {
			c.store.PutConversation(&resp.Items[i])
			total++
		}
		if resp.NextCursor == nil || *resp.NextCursor == "" {
			break
		}
		dCursor = *resp.NextCursor
	}

	// 3) 增量 sync：从 recent 给出的 version 起
	since := version
	for {
		q := url.Values{"limit": {"500"}, "since_version": {strconv.FormatInt(since, 10)}}
		if epoch != "" {
			q.Set("sync_epoch", epoch)
		}
		var resp struct {
			SyncEpoch       string              `json:"sync_epoch"`
			ResetRequired   bool                `json:"reset_required"`
			RecentRequired  bool                `json:"recent_required"`
			Items           []ConversationState `json:"items"`
			DeletedMessages []struct {
				ConversationID string  `json:"conversation_id"`
				Seqs           []int64 `json:"seqs"`
			} `json:"deleted_messages"`
			NextVersion int64 `json:"next_version"`
			HasMore     bool  `json:"has_more"`
		}
		if err := c.getJSON(ctx, "/client/v1/conversations/sync", q, &resp); err != nil {
			return total, err
		}
		if resp.ResetRequired || resp.RecentRequired {
			// 数据回退或落后太多：重跑一遍全量（recent → digest → sync），只重试一次防死循环。
			if c.syncRetried {
				return total, nil
			}
			c.syncRetried = true
			n, err := c.SyncAll(ctx)
			c.syncRetried = false
			return total + n, err
		}
		for i := range resp.Items {
			c.store.PutConversation(&resp.Items[i])
			total++
		}
		for _, d := range resp.DeletedMessages {
			c.store.DeleteMessages(d.ConversationID, d.Seqs)
		}
		if resp.SyncEpoch != "" {
			epoch = resp.SyncEpoch
		}
		since = resp.NextVersion
		if !resp.HasMore {
			break
		}
	}

	// 4) 各会话补最后一条消息（recent 未覆盖到的会话）
	if err := c.BackfillLastMessages(ctx); err != nil {
		return total, err
	}
	return total, nil
}

// syncRecent 拉取最近的会话列表（第 7.3 节第 1 步）：pinned + items 翻页取完，
// 返回同步数和首页给出的 version / sync_epoch（作为增量 sync 的起点）。
func (c *Client) syncRecent(ctx context.Context) (total int, version int64, epoch string, err error) {
	cursor := ""
	first := true
	for {
		q := url.Values{"limit": {"100"}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		var resp struct {
			Version    *int64        `json:"version"`
			SyncEpoch  *string       `json:"sync_epoch"`
			Pinned     []recentEntry `json:"pinned"`
			Items      []recentEntry `json:"items"`
			NextCursor *string       `json:"next_cursor"`
		}
		if err := c.getJSON(ctx, "/client/v1/conversations/recent", q, &resp); err != nil {
			return total, version, epoch, err
		}
		entries := append(append([]recentEntry{}, resp.Pinned...), resp.Items...)
		for _, e := range entries {
			st := e.Conversation
			c.store.PutConversation(&st)
			total++
			if e.LastMessage != nil {
				c.store.PutMessage(e.LastMessage)
			}
		}
		if first {
			first = false
			if resp.Version != nil {
				version = *resp.Version
			}
			if resp.SyncEpoch != nil {
				epoch = *resp.SyncEpoch
			}
		}
		if resp.NextCursor == nil || *resp.NextCursor == "" {
			return total, version, epoch, nil
		}
		cursor = *resp.NextCursor
	}
}

// BackfillLastMessages 用 last-messages 把每个会话的最后一条消息拉进缓存。
// 服务端 items 形如 {conversation_id, message}（message 可能为 null）。
func (c *Client) BackfillLastMessages(ctx context.Context) error {
	for _, st := range c.store.Conversations() {
		if st.MaxSeq == 0 {
			continue
		}
		ids := []string{st.ConversationID}
		var resp struct {
			Items []struct {
				ConversationID string   `json:"conversation_id"`
				Message        *Message `json:"message"`
			} `json:"items"`
		}
		if err := c.request(ctx, "POST", "/client/v1/conversations/last-messages", map[string]any{"conversation_ids": ids}, &resp); err != nil {
			continue
		}
		for _, it := range resp.Items {
			if it.Message != nil {
				c.store.PutMessage(it.Message)
			}
		}
	}
	return nil
}

// lightSync 轻量同步（sync_hint / resync 触发）：重跑 conversations/sync 增量。
func (c *Client) lightSync() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = c.SyncAll(ctx)
	c.logf("debug", "sync", "轻量同步完成", nil)
}

// FetchMessages 拉取会话历史。afterSeq>0 时从该 seq 之后拉（补缺口）；
// beforeSeq>0 时从该 seq 之前往前拉（历史翻页）；两者都 0 时从最新往回拉。
// limit<=0 时用默认 50。
func (c *Client) FetchMessages(conversationID string, afterSeq, beforeSeq, limit int64) ([]*Message, error) {
	return c.fetchMessages(context.Background(), conversationID, afterSeq, beforeSeq, limit)
}

func (c *Client) fetchMessages(ctx context.Context, conversationID string, afterSeq, beforeSeq, limit int64) ([]*Message, error) {
	if limit <= 0 {
		limit = 50
	}
	q := url.Values{"limit": {strconv.FormatInt(limit, 10)}}
	if afterSeq > 0 {
		q.Set("after_seq", strconv.FormatInt(afterSeq, 10))
		q.Set("catch_up", "true")
	}
	if beforeSeq > 0 {
		q.Set("before_seq", strconv.FormatInt(beforeSeq, 10))
	}
	var resp struct {
		Items      []*Message `json:"items"`
		NextCursor string     `json:"next_cursor,omitempty"`
	}
	if err := c.getJSON(ctx, "/client/v1/conversations/"+conversationID+"/messages", q, &resp); err != nil {
		return nil, err
	}
	for _, m := range resp.Items {
		c.store.PutMessage(m)
	}
	return resp.Items, nil
}

// History 往前翻页拉取历史消息（beforeSeq 之前），自动写入 Store。
func (c *Client) History(conversationID string, beforeSeq int64, limit int64) ([]*Message, error) {
	return c.fetchMessages(context.Background(), conversationID, 0, beforeSeq, limit)
}
