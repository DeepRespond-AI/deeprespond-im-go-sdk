package im

// 服务端事件（《网关 3.接口设计》第 4 节）的封装与分发。
// 未知 event 名不报错：SDK 必须忽略不认识的事件，RawData 保留原文供上层处理。

import (
	"encoding/json"
	"time"
)

// 服务端事件名（《网关 3.接口设计》第 10 节；与网关 domain/notify.go 保持一致）。
const (
	EventMessageReceived     = "message.received"
	EventMessageChanged      = "message.changed"
	EventMessageDeleted      = "message.deleted"
	EventMessageOnline       = "message.online"
	EventConversationChanged = "message.conversation_changed"
	EventPeerRead            = "message.peer_read"
	EventReadBy              = "message.read_by"
	EventTyping              = "message.typing"
	EventSyncHint            = "message.sync_hint"

	EventFriendAdded           = "friend.added"
	EventFriendUpdated         = "friend.updated"
	EventFriendRemoved         = "friend.removed"
	EventFriendRequestReceived = "friend_request.received"
	EventFriendRequestDeclined = "friend_request.declined"
	EventFriendRequestRead     = "friend_request.read"
	EventBlacklistChanged      = "blacklist.changed"

	EventGroupCreated        = "group.created"
	EventGroupInfoChanged    = "group.info_changed"
	EventGroupMembersAdded   = "group.members_added"
	EventGroupMembersRemoved = "group.members_removed"
	EventGroupMembersUpdated = "group.members_updated"
	EventGroupDismissed      = "group.dismissed"
	EventGroupRequestCreated = "group.request_created"
	EventGroupRequestHandled = "group.request_handled"

	EventUserProfileChanged  = "user.profile_changed"
	EventUserSessionsRevoked = "user.sessions_revoked"
	EventUserReloginRequired = "user.relogin_required"
	EventUserStatusChanged   = "user.status_changed"
	EventUserMuteChanged     = "user.mute_changed"

	EventPresenceChanged = "presence.changed"
	EventPresenceReset   = "presence.reset"
	EventDevicesChanged  = "devices.changed"
	EventConfigChanged   = "config.changed"
	EventAuthExpired     = "auth.expired"
	EventSyncRequired    = "sync.required"
	EventPolicyChanged   = "policy.changed"
)

// Event 一条服务端事件。
type Event struct {
	Name string          `json:"event"`
	ID   string          `json:"id"` // 事件 ID，用于去重
	At   string          `json:"at"` // 事件时间
	Data json.RawMessage `json:"-"`
	Raw  json.RawMessage `json:"-"` // 整帧原文
}

// 事件 data 结构。

type MessageEventData struct {
	Message
	// 服务端在 message.received 里的 data 就是消息 JSON（内嵌上面的字段）。
}

// MessageChangedData 是 message.changed 的载荷：服务端不带整条消息，只带变化的字段（平铺在顶层）。
type MessageChangedData struct {
	ConversationID string        `json:"conversation_id"`
	Seq            int64         `json:"seq"`
	MessageVersion int64         `json:"message_version"`
	Change         MessageChange `json:"change"`
	// 变化字段（按 change.kind 出现）：recalled → Recalled；edited → Body/Ext/Edited；
	// reaction_* → Reactions；pinned/unpinned → Pinned；erased → Erased。
	Recalled  *Recalled       `json:"recalled,omitempty"`
	Edited    *Edited         `json:"edited,omitempty"`
	Pinned    *Pinned         `json:"pinned,omitempty"`
	Reactions []Reaction      `json:"reactions,omitempty"`
	Erased    bool            `json:"erased,omitempty"`
	Body      json.RawMessage `json:"body,omitempty"`
	Ext       json.RawMessage `json:"ext,omitempty"`
}

type MessageChange struct {
	Kind  string `json:"kind"` // recalled|edited|erased|reaction_added|reaction_removed|pinned|unpinned
	Actor string `json:"actor,omitempty"`
	Key   string `json:"key,omitempty"` // reaction 的 key
}

type MessageDeletedData struct {
	ConversationID string  `json:"conversation_id"`
	Seqs           []int64 `json:"seqs"`
	Version        int64   `json:"version,omitempty"`
}

type PeerReadData struct {
	ConversationID string `json:"conversation_id"`
	Peer           string `json:"peer"`
	ReadSeq        int64  `json:"read_seq"`
}

type ReadByItem struct {
	Reader string  `json:"reader"`
	Seqs   []int64 `json:"seqs"`
}

type ReadByData struct {
	ConversationID string       `json:"conversation_id"`
	Items          []ReadByItem `json:"items"`
	Counts         []struct {
		Seq         int64 `json:"seq"`
		ReadCount   int64 `json:"read_count"`
		UnreadCount int64 `json:"unread_count"`
	} `json:"counts,omitempty"`
}

type TypingData struct {
	ConversationID string `json:"conversation_id"`
	Username       string `json:"username"`
}

type SyncHintData struct {
	ConversationIDs []string `json:"conversation_ids"` // null 表示全量
}

type FriendEventData struct {
	Username        string         `json:"username"`        // 所有者
	FriendUsername  string         `json:"friend_username"` // 对端用户名（added/removed/updated）
	FriendNickname  string         `json:"friend_nickname,omitempty"`
	FriendAvatarURL string         `json:"friend_avatar_url,omitempty"`
	Remark          string         `json:"remark,omitempty"`
	Attributes      map[string]any `json:"attributes,omitempty"`
	AddSource       string         `json:"add_source,omitempty"`
	CreatedAt       string         `json:"created_at,omitempty"`
	FriendCreatedAt string         `json:"friend_created_at,omitempty"` // removed
	Reason          string         `json:"reason,omitempty"`            // removed
	Source          string         `json:"source,omitempty"`            // added
	Version         int64          `json:"version,omitempty"`
	Origin          string         `json:"origin,omitempty"`
}

// FriendRequestEventData friend_request.* 的公共字段（received/declined）。
type FriendRequestEventData struct {
	FromUserID   int64  `json:"from_user_id,string,omitempty"`
	FromUsername string `json:"from_username,omitempty"`
	ToUserID     int64  `json:"to_user_id,string,omitempty"`
	ToUsername   string `json:"to_username,omitempty"`
	Message      string `json:"message,omitempty"`      // received
	AddSource    string `json:"add_source,omitempty"`   // received
	RequestedAt  string `json:"requested_at,omitempty"` // received
	HandledAt    string `json:"handled_at,omitempty"`   // declined
	Origin       string `json:"origin,omitempty"`
}

// FriendRequestReadData friend_request.read：已读位置前进，其他设备据此刷新未读数。
type FriendRequestReadData struct {
	Username  string `json:"username"`
	ReadUntil string `json:"read_until"`
	Origin    string `json:"origin,omitempty"`
}

// BlacklistChangedData blacklist.changed：Blocked 为 false 表示移出黑名单。
type BlacklistChangedData struct {
	Username string `json:"username"` // 对方用户名
	Blocked  bool   `json:"blocked"`
	Version  int64  `json:"version,omitempty"`
	Origin   string `json:"origin,omitempty"`
}

// GroupEventData group.* 事件的通用载荷形态（群 ID 一律字符串）。
type GroupEventData struct {
	GroupID       string          `json:"group_id"`
	Info          json.RawMessage `json:"info,omitempty"`           // info_changed：完整群信息
	ChangedFields []string        `json:"changed_fields,omitempty"` // info_changed
	Username      string          `json:"username,omitempty"`       // members_*：涉及成员
	Role          string          `json:"role,omitempty"`           // members_updated
	Reason        string          `json:"reason,omitempty"`         // members_removed / dismissed
	Version       int64           `json:"version,omitempty"`
	Origin        string          `json:"origin,omitempty"`
}

// UserProfileChangedData user.profile_changed：仅本人资料变化。
type UserProfileChangedData struct {
	Username   string         `json:"username"`
	Nickname   string         `json:"nickname,omitempty"`
	AvatarURL  string         `json:"avatar_url,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
	Origin     string         `json:"origin,omitempty"`
}

// dispatch 把 event 帧交给 Store 应用并回调注册的 handler。
func (w *wsManager) dispatch(f *wsFrame) {
	w.evMu.RLock()
	handlers := make([]EventHandler, len(w.handlers))
	copy(handlers, w.handlers)
	w.evMu.RUnlock()

	e := Event{Name: f.Event, ID: f.ID, At: f.At, Data: f.Data}

	// 先应用到本地缓存（顺序化），再回调上层
	if w.c.store != nil {
		w.c.store.applyEvent(w.c, &e)
	}
	for _, h := range handlers {
		func(h EventHandler) {
			defer func() { _ = recover() }() // 回调 panic 不影响连接
			h(e)
		}(h)
	}
}

// Time 解析事件时间。
func (e *Event) Time() time.Time {
	t, _ := time.Parse(time.RFC3339, e.At)
	return t
}
