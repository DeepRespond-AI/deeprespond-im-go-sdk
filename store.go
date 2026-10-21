package im

// 内存会话/消息缓存与事件应用规则（《消息 3.接口设计》第 6.7 节）：
// message.received 按 seq 与本地 max_seq 比较 —— 等于 max+1 直接追加；
// 更大用 after_seq 补齐；更小忽略。cleared_seq 变大删除本地不超过它的消息。

import (
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// TypingInfo 一次输入中通知。
type TypingInfo struct {
	ConversationID string
	Username       string
	At             time.Time
}

// Store 客户端内存缓存。长连接开启后自动维护；REST 拉取的结果也可写入。
type Store struct {
	mu   sync.RWMutex
	self string

	convs       map[string]*ConversationState
	messages    map[string]map[int64]*Message // conversation_id → seq → message
	lastEventID string
	typing      map[string]TypingInfo // conversation_id → 最近输入中
	fetching    map[string]bool       // 补齐中的会话，防重复拉取
}

func newStore() *Store {
	return &Store{
		convs:    map[string]*ConversationState{},
		messages: map[string]map[int64]*Message{},
		typing:   map[string]TypingInfo{},
		fetching: map[string]bool{},
	}
}

func (s *Store) setSelf(username string) {
	s.mu.Lock()
	s.self = username
	s.mu.Unlock()
}

// Self 返回当前用户名。
func (s *Store) Self() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.self
}

// Conversation 返回指定会话状态快照。
func (s *Store) Conversation(id string) *ConversationState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if c, ok := s.convs[id]; ok {
		cp := *c
		return &cp
	}
	return nil
}

// Conversations 返回全部会话快照，按最近消息时间倒序。
func (s *Store) Conversations() []*ConversationState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*ConversationState, 0, len(s.convs))
	for _, c := range s.convs {
		cp := *c
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		ti, tj := out[i].LastMessageAt, out[j].LastMessageAt
		if ti != tj {
			return ti > tj
		}
		return out[i].ConversationID < out[j].ConversationID
	})
	return out
}

// Messages 返回会话内缓存的全部消息（seq 升序）。
func (s *Store) Messages(conversationID string) []*Message {
	s.mu.RLock()
	defer s.mu.RUnlock()
	bySeq := s.messages[conversationID]
	out := make([]*Message, 0, len(bySeq))
	for _, m := range bySeq {
		cp := *m
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// LastMessages 返回会话内最近 limit 条消息（升序）。
func (s *Store) LastMessages(conversationID string, limit int) []*Message {
	all := s.Messages(conversationID)
	if len(all) > limit {
		all = all[len(all)-limit:]
	}
	return all
}

// Typing 返回会话内最近的输入中通知（10 秒内有效）。
func (s *Store) Typing(conversationID string) *TypingInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if t, ok := s.typing[conversationID]; ok && time.Since(t.At) < 10*time.Second {
		return &t
	}
	return nil
}

// PutConversation 写入会话状态（同步流程使用）。
func (s *Store) PutConversation(c *ConversationState) {
	s.mu.Lock()
	s.convs[c.ConversationID] = c
	s.mu.Unlock()
}

// PutMessage 写入一条消息。
func (s *Store) PutMessage(m *Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.putMessageLocked(m)
}

// DeleteMessages 从缓存移除指定 seq 的消息（sync 的 deleted_messages）。
func (s *Store) DeleteMessages(conversationID string, seqs []int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bySeq := s.messages[conversationID]
	if bySeq == nil {
		return
	}
	for _, seq := range seqs {
		delete(bySeq, seq)
	}
	if len(bySeq) == 0 {
		delete(s.messages, conversationID)
	}
}

// upsertSentConv 发送成功后本地兜底会话状态：服务端对发送者不下发 message.received，
// 新会话的 conversation_changed 也可能不达发送者；不补这条，发送者自己的
// 已读回执（peer_read）等会话级簿记就无处落地。
func (s *Store) upsertSentConv(m *Message, target Target) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.convs[m.ConversationID]; ok {
		if m.Seq > st.MaxSeq {
			st.MaxSeq = m.Seq
			st.LastMessageAt = m.CreatedAt
		}
		return
	}
	st := &ConversationState{
		ConversationID:   m.ConversationID,
		ConversationType: m.ConversationType,
		MaxSeq:           m.Seq,
		StartSeq:         m.Seq,
		LastMessageAt:    m.CreatedAt,
	}
	if m.ConversationType == "single" {
		if m.Sender == s.self {
			st.Peer = m.Recipient
		} else {
			st.Peer = m.Sender
		}
	} else if target.kind == "group" {
		st.GroupID = target.id
	}
	s.convs[m.ConversationID] = st
}

func (s *Store) putMessageLocked(m *Message) {
	bySeq := s.messages[m.ConversationID]
	if bySeq == nil {
		bySeq = map[int64]*Message{}
		s.messages[m.ConversationID] = bySeq
	}
	bySeq[m.Seq] = m
}

// ---- 事件应用 ----

// applyEvent 把一条服务端事件应用到缓存。
func (s *Store) applyEvent(c *Client, e *Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.ID != "" && e.ID == s.lastEventID {
		return // 事件去重
	}
	s.lastEventID = e.ID

	switch e.Name {
	case EventMessageReceived:
		var m Message
		if json.Unmarshal(e.Data, &m) != nil || m.Seq == 0 {
			return
		}
		st := s.convs[m.ConversationID]
		maxSeq := int64(0)
		if st != nil {
			maxSeq = st.MaxSeq
		}
		switch {
		case m.Seq == maxSeq+1 || st == nil:
			s.putMessageLocked(&m)
			if st == nil {
				st = &ConversationState{
					ConversationID:   m.ConversationID,
					ConversationType: m.ConversationType,
					MaxSeq:           m.Seq,
					StartSeq:         m.Seq,
				}
				if m.ConversationType == "single" {
					if m.Sender == s.self {
						st.Peer = m.Recipient
					} else {
						st.Peer = m.Sender
					}
				}
				s.convs[m.ConversationID] = st
			} else {
				st.MaxSeq = m.Seq
				st.LastMessageAt = m.CreatedAt
			}
			if m.Sender != s.self && !m.ExcludeFromUnread {
				st.UnreadCount++
			}
		case m.Seq > maxSeq+1:
			// 有缺口：先把这条收下，再异步补齐缺口
			s.putMessageLocked(&m)
			st.MaxSeq = m.Seq
			st.LastMessageAt = m.CreatedAt
			if m.Sender != s.self && !m.ExcludeFromUnread {
				st.UnreadCount++
			}
			s.fetchMissingAsync(c, m.ConversationID, maxSeq+1)
		default:
			// 过期事件：忽略
		}

	case EventMessageChanged:
		var d MessageChangedData
		if json.Unmarshal(e.Data, &d) != nil {
			return
		}
		bySeq := s.messages[d.ConversationID]
		if bySeq == nil {
			return
		}
		m := bySeq[d.Seq]
		if m == nil || m.MessageVersion >= d.MessageVersion {
			return
		}
		// message.changed 只带变化字段，按 kind 合并进本地消息。
		if d.Recalled != nil {
			m.Recalled = d.Recalled
			m.Body, m.Ext, m.Mentions, m.ReplyTo, m.Reactions, m.Pinned = nil, nil, nil, nil, nil, nil
		}
		if d.Edited != nil {
			m.Edited = d.Edited
		}
		if d.Body != nil {
			m.Body = d.Body
		}
		if d.Ext != nil {
			_ = json.Unmarshal(d.Ext, &m.Ext)
		}
		if d.Pinned != nil {
			m.Pinned = d.Pinned
		}
		switch d.Change.Kind {
		case "unpinned":
			m.Pinned = nil
		case "erased":
			m.Erased = true
			m.Body, m.Ext = nil, nil
		}
		if len(d.Reactions) > 0 {
			m.Reactions = d.Reactions
		}
		m.MessageVersion = d.MessageVersion

	case EventMessageDeleted:
		var d MessageDeletedData
		if json.Unmarshal(e.Data, &d) != nil {
			return
		}
		bySeq := s.messages[d.ConversationID]
		if bySeq == nil {
			return
		}
		for _, seq := range d.Seqs {
			delete(bySeq, seq)
		}

	case EventConversationChanged:
		// data 可能是会话状态 JSON，或 {"resync":true}
		var st ConversationState
		if json.Unmarshal(e.Data, &st) == nil && st.ConversationID != "" {
			if cur := s.convs[st.ConversationID]; cur == nil || st.Version >= cur.Version {
				s.convs[st.ConversationID] = &st
				// cleared_seq 变大：删除本地不超过它的消息
				if cur != nil && st.ClearedSeq > cur.ClearedSeq {
					bySeq := s.messages[st.ConversationID]
					for seq := range bySeq {
						if seq <= st.ClearedSeq {
							delete(bySeq, seq)
						}
					}
				}
			}
		} else {
			var rs struct {
				Resync bool `json:"resync"`
			}
			if json.Unmarshal(e.Data, &rs) == nil && rs.Resync {
				go c.lightSync()
			}
		}

	case EventPeerRead:
		var d PeerReadData
		if json.Unmarshal(e.Data, &d) != nil {
			return
		}
		if st := s.convs[d.ConversationID]; st != nil && d.ReadSeq > st.PeerReadSeq {
			st.PeerReadSeq = d.ReadSeq
		}

	case EventTyping:
		var d TypingData
		if json.Unmarshal(e.Data, &d) != nil {
			return
		}
		s.typing[d.ConversationID] = TypingInfo{
			ConversationID: d.ConversationID, Username: d.Username, At: time.Now(),
		}

	case EventSyncHint, EventSyncRequired:
		go c.lightSync()
	}
}

// fetchMissingAsync 异步拉取会话缺口消息（after_seq 起到本地 max）。
func (s *Store) fetchMissingAsync(c *Client, conversationID string, fromSeq int64) {
	if s.fetching[conversationID] {
		return
	}
	s.fetching[conversationID] = true
	go func() {
		defer func() {
			s.mu.Lock()
			s.fetching[conversationID] = false
			s.mu.Unlock()
		}()
		msgs, err := c.FetchMessages(conversationID, fromSeq, 0, 100)
		if err != nil {
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, m := range msgs {
			s.putMessageLocked(m)
		}
	}()
}
