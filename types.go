package im

import "encoding/json"

// ---- 会话与目标 ----

// Target 描述消息发送目标。用 ToUser / ToGroup 构造。
type Target struct {
	kind string // "user" | "group"
	id   string
}

func ToUser(username string) Target { return Target{kind: "user", id: username} }
func ToGroup(groupID string) Target { return Target{kind: "group", id: groupID} }

// ---- 用户 ----

// UserProfile 用户公开资料。
type UserProfile struct {
	Username   string         `json:"username"`
	Nickname   string         `json:"nickname"`
	AvatarURL  string         `json:"avatar_url,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
	Version    int64          `json:"version,omitempty"`
}

// SelfProfile 本人资料，含密码与可见性等私有信息。
type SelfProfile struct {
	UserProfile
	HasPassword bool `json:"has_password"`
}

// LoginSession 一次登录会话（多端设备）。
type LoginSession struct {
	SessionID    string `json:"session_id"`
	Platform     string `json:"platform,omitempty"`
	DeviceName   string `json:"device_name,omitempty"`
	DeviceID     string `json:"device_id,omitempty"`
	CreatedAt    string `json:"created_at,omitempty"`
	LastActiveAt string `json:"last_active_at,omitempty"`
	Current      bool   `json:"current,omitempty"`
}

// PresenceItem 在线状态查询/订阅结果的一项。
type PresenceItem struct {
	Username   string   `json:"username"`
	Online     bool     `json:"online"`
	Platforms  []string `json:"platforms,omitempty"`
	LastSeenAt string   `json:"last_seen_at,omitempty"`
	Version    int64    `json:"version,omitempty"`
}

// ---- 好友 ----

// Friend 好友关系（双向视角一致，remark/attributes 是本人视角）。
type Friend struct {
	Username      string         `json:"username"`
	Nickname      string         `json:"nickname,omitempty"`
	AvatarURL     string         `json:"avatar_url,omitempty"`
	Remark        string         `json:"remark,omitempty"`
	Attributes    map[string]any `json:"attributes,omitempty"`
	AddSource     string         `json:"add_source,omitempty"`
	CreatedAt     string         `json:"created_at,omitempty"`
	FriendVersion int64          `json:"friend_version,omitempty"`
}

// FriendRequest 好友申请（对端视角：received 时 Username 为申请人，sent 时为受邀人）。
type FriendRequest struct {
	Username    string  `json:"username"`
	Nickname    string  `json:"nickname,omitempty"`
	AvatarURL   string  `json:"avatar_url,omitempty"`
	Message     string  `json:"message,omitempty"` // 申请附言
	Remark      *string `json:"remark,omitempty"`  // sent：申请人预设的备注
	AddSource   string  `json:"add_source,omitempty"`
	Status      string  `json:"status,omitempty"` // pending|accepted|declined
	Unread      *bool   `json:"unread,omitempty"`
	RequestedAt string  `json:"requested_at,omitempty"`
	ExpiresAt   string  `json:"expires_at,omitempty"`
	HandledAt   string  `json:"handled_at,omitempty"`
}

// FriendCheck 好友关系检查结果。
type FriendCheck struct {
	Username string `json:"username"`
	IsFriend bool   `json:"is_friend"`
}

// ---- 群 ----

// GroupInfo 群信息。
type GroupInfo struct {
	GroupID       string `json:"group_id"`
	Name          string `json:"name,omitempty"`
	AvatarURL     string `json:"avatar_url,omitempty"`
	Description   string `json:"description,omitempty"`
	Owner         string `json:"owner,omitempty"`
	MemberCount   int64  `json:"member_count,omitempty"`
	Public        bool   `json:"public,omitempty"`
	JoinMode      string `json:"join_mode,omitempty"`
	MyRole        string `json:"my_role,omitempty"` // owner|admin|member
	InfoVersion   int64  `json:"info_version,omitempty"`
	MemberVersion int64  `json:"member_version,omitempty"`
	CreatedAt     string `json:"created_at,omitempty"`
}

// GroupMember 群成员。
type GroupMember struct {
	Username  string `json:"username"`
	Nickname  string `json:"nickname,omitempty"`
	AvatarURL string `json:"avatar_url,omitempty"`
	Role      string `json:"role,omitempty"`
	JoinedAt  string `json:"joined_at,omitempty"`
}

// GroupMemberResult 批量成员变更的单项结果。
type GroupMemberResult struct {
	Username string    `json:"username"`
	Result   string    `json:"result,omitempty"` // ok|already_member|not_member|...
	Error    *APIError `json:"error,omitempty"`
}

// GroupInvitation 收到的群邀请。
type GroupInvitation struct {
	InvitationID string `json:"invitation_id,omitempty"`
	GroupID      string `json:"group_id,omitempty"`
	GroupName    string `json:"group_name,omitempty"`
	Inviter      string `json:"inviter,omitempty"`
	CreatedAt    string `json:"created_at,omitempty"`
}

// ---- 消息 ----

// Mentions 提醒对象。
type Mentions struct {
	Usernames []string `json:"usernames,omitempty"`
	All       bool     `json:"all,omitempty"`
}

// ReplyTo 回复目标。
type ReplyTo struct {
	Seq    int64  `json:"seq"`
	Sender string `json:"sender,omitempty"`
}

// Reaction 消息回应聚合。
type Reaction struct {
	Key     string   `json:"key"`
	Count   int64    `json:"count"`
	Users   []string `json:"users,omitempty"`   // 前 3 个用户，删除者为 null
	Reacted bool     `json:"reacted,omitempty"` // 本人是否已回应
}

// Pinned / Edited / Recalled 消息状态标记。
type Pinned struct {
	By string `json:"by"`
	At string `json:"at"`
}
type Edited struct {
	At    string `json:"at"`
	Count int64  `json:"count,omitempty"`
}
type Recalled struct {
	By   string `json:"by"`
	Role string `json:"role,omitempty"`
	At   string `json:"at"`
}

// Message 消息（《消息 3.接口设计》第 5 节）。Body 按 Type 解释：
// text → {"text": "..."}；image/file/voice/video → 上传返回的 FileInfo JSON；
// custom / tip → 自定义对象。可用 ParseText / FileInfoBody 提取。
type Message struct {
	ConversationID    string          `json:"conversation_id"`
	ConversationType  string          `json:"conversation_type"` // single|group
	Seq               int64           `json:"seq"`
	MessageID         string          `json:"message_id"`
	ClientMsgID       string          `json:"client_msg_id,omitempty"`
	Sender            string          `json:"sender"`
	SenderType        string          `json:"sender_type,omitempty"` // user|system
	Recipient         string          `json:"recipient,omitempty"`
	Type              string          `json:"type"`
	Body              json.RawMessage `json:"body"`
	Ext               map[string]any  `json:"ext,omitempty"`
	Mentions          *Mentions       `json:"mentions,omitempty"`
	ReplyTo           *ReplyTo        `json:"reply_to,omitempty"`
	NeedReceipt       bool            `json:"need_receipt,omitempty"`
	ExcludeFromUnread bool            `json:"exclude_from_unread,omitempty"`
	Reactions         []Reaction      `json:"reactions,omitempty"`
	Pinned            *Pinned         `json:"pinned,omitempty"`
	Edited            *Edited         `json:"edited,omitempty"`
	Recalled          *Recalled       `json:"recalled,omitempty"`
	Erased            bool            `json:"erased,omitempty"`
	Via               string          `json:"via,omitempty"` // client|openapi|console|system
	CreatedAt         string          `json:"created_at"`
	MessageVersion    int64           `json:"message_version,omitempty"`
}

// TextBody 提取 text 消息正文；非 text 消息返回空串。
func (m *Message) TextBody() string {
	var b struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(m.Body, &b) == nil {
		return b.Text
	}
	return ""
}

// BodyMap 把 body 解析为通用对象（custom/tip 等）。
func (m *Message) BodyMap() map[string]any {
	var b map[string]any
	if json.Unmarshal(m.Body, &b) == nil {
		return b
	}
	return nil
}

// SendInput 发送消息入参。Target 用 ToUser/ToGroup 恰选其一；Type 缺省为 text。
type SendInput struct {
	Target            Target
	Type              string
	Text              string          // text 消息直接给正文
	Body              json.RawMessage // 非 text 消息给完整 body（如 FileInfo）
	Ext               map[string]any
	Mentions          *Mentions
	ReplyToSeq        int64
	NeedReceipt       bool
	ExcludeFromUnread bool
	ClientMsgID       string // 缺省自动生成
	OnlineOnly        bool   // 仅 custom 消息可用，不落库
}

// ---- 会话 ----

// ConversationState 会话状态（《消息 3.接口设计》第 6 节）。
type ConversationState struct {
	ConversationID   string  `json:"conversation_id"`
	ConversationType string  `json:"conversation_type"`
	Peer             string  `json:"peer,omitempty"`
	GroupID          string  `json:"group_id,omitempty"`
	Membership       *string `json:"membership"` // member|left|removed|dismissed|null
	StartSeq         int64   `json:"start_seq"`
	MaxSeq           int64   `json:"max_seq"`
	ReadSeq          int64   `json:"read_seq"`
	UnreadCount      int64   `json:"unread_count"`
	MarkedUnread     bool    `json:"marked_unread"`
	PeerReadSeq      int64   `json:"peer_read_seq"`
	MentionSeq       int64   `json:"mention_seq"`
	ClearedSeq       int64   `json:"cleared_seq"`
	HiddenSeq        int64   `json:"hidden_seq"`
	Hidden           bool    `json:"hidden"`
	PinnedAt         string  `json:"pinned_at,omitempty"`
	JoinedAt         string  `json:"joined_at,omitempty"`
	LeftAt           string  `json:"left_at,omitempty"`
	MessageVersion   int64   `json:"message_version,omitempty"`
	LastMessageAt    string  `json:"last_message_at,omitempty"`
	Version          int64   `json:"version"`
}

// ---- 媒体 ----

// FileInfo 上传完成后的文件信息，可直接作为 image/file/voice/video 消息 body。
type FileInfo struct {
	FileID       string `json:"file_id"`
	Name         string `json:"name,omitempty"`
	Size         int64  `json:"size,omitempty"`
	ContentType  string `json:"content_type,omitempty"`
	Width        int64  `json:"width,omitempty"`
	Height       int64  `json:"height,omitempty"`
	DurationMs   int64  `json:"duration_ms,omitempty"`
	URL          string `json:"url,omitempty"`
	ThumbnailURL string `json:"thumbnail_url,omitempty"`
	SHA256       string `json:"sha256,omitempty"`
}

// PutTarget 创建上传后返回的预签名直传目标。
type PutTarget struct {
	URL     string            `json:"url"`
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

// DownloadURL 换取下载地址的一项：成功时有 DownloadURL（签名直链）与过期时间；
// 失败时 URL 为原地址、DownloadURL 为空并带 code/message。
type DownloadURL struct {
	URL         string `json:"url"`                    // 原始媒体地址
	DownloadURL string `json:"download_url,omitempty"` // 签名直链（真正可 GET 的地址）
	ExpiresAt   string `json:"expires_at,omitempty"`
	Code        string `json:"code,omitempty"`
	Message     string `json:"message,omitempty"`
}

// ---- 登录 ----

// Session 登录/续期成功后的完整凭据与本人资料。
type Session struct {
	AccessToken           string       `json:"access_token"`
	AccessTokenExpiresIn  *int64       `json:"access_token_expires_in"`
	RefreshToken          string       `json:"refresh_token"`
	RefreshTokenExpiresIn *int64       `json:"refresh_token_expires_in"`
	SessionID             string       `json:"session_id"`
	User                  *SelfProfile `json:"user,omitempty"`
	ConfigVersion         int64        `json:"config_version,omitempty"`
}

// Page 泛型分页返回。
type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
	Total      int64  `json:"total,omitempty"`
}
