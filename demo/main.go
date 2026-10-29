// Go SDK Demo：单进程 Web 即时通讯应用。
//
// 架构：浏览器 ⇄ demo 后端（HTTP + WebSocket 桥）⇄ im.Client（本 SDK）⇄ IM 服务端。
// 每个浏览器会话（cookie demo_sid）持有一个独立的 im.Client，SDK 事件经桥实时
// 推送到页面。除标准库外零依赖（WebSocket 桥为内置的最小 RFC6455 服务端实现）。
//
// 启动：
//
//	go run ./demo --api http://127.0.0.1:18480 --app-key "org#app" --addr :8090
package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"io/fs"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	im "github.com/DeepRespond-AI/deeprespond-im-go-sdk"
)

//go:embed web
var webFS embed.FS

var (
	flagAddr   = flag.String("addr", ":8090", "HTTP 监听地址")
	flagAPI    = flag.String("api", "http://127.0.0.1:18480", "IM 服务端 REST 地址")
	flagAppKey = flag.String("app-key", os.Getenv("APP_KEY"), "应用 AppKey（组织#应用）")
	flagDebug  = flag.Bool("debug", false, "打印 SDK 调试日志")
)

const cookieName = "demo_sid"

// ---------------------------------------------------------------------------
// 浏览器会话

type session struct {
	id      string
	c       *im.Client
	mu      sync.Mutex
	br      *bridgeConn
	dead    bool
	kickFcm bool // kicked 推送只发一次
	syncing bool // resyncConvs 重入保护
	friends map[string]im.Friend
	groups  map[string]im.GroupInfo
}

type sessionTable struct {
	mu sync.Mutex
	m  map[string]*session
}

var sessions = &sessionTable{m: map[string]*session{}}

func (t *sessionTable) get(r *http.Request) *session {
	ck, err := r.Cookie(cookieName)
	if err != nil || ck.Value == "" {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.m[ck.Value]
}

func (t *sessionTable) put(s *session) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.m[s.id] = s
}

func (t *sessionTable) drop(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.m, id)
}

// ---------------------------------------------------------------------------
// 入口与路由

func main() {
	flag.Parse()
	if *flagAppKey == "" {
		log.Fatal("缺少 --app-key（或环境变量 APP_KEY），形如 6526900072#myapp")
	}

	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(sub)))
	mux.HandleFunc("/api/login", handleLogin)
	mux.HandleFunc("/api/logout", handleLogout)
	mux.HandleFunc("/api/upload", handleUpload)
	mux.HandleFunc("/api/media", handleMedia)
	mux.HandleFunc("/ws", handleWS)

	log.Printf("Go SDK demo 已启动: http://0.0.0.0%s  (api=%s app_key=%s)", *flagAddr, *flagAPI, *flagAppKey)
	server := &http.Server{Addr: *flagAddr, Handler: mux}
	log.Fatal(server.ListenAndServe())
}

func sdkLogger() im.Logger {
	if !*flagDebug {
		return nil
	}
	return consoleLogger{}
}

// consoleLogger 把 SDK 日志写到标准输出（实现 im.Logger）。
type consoleLogger struct{}

func (consoleLogger) Log(level, module, message string, fields map[string]any) {
	if len(fields) == 0 {
		log.Printf("[%s] %s: %s", level, module, message)
		return
	}
	b, _ := json.Marshal(fields)
	log.Printf("[%s] %s: %s %s", level, module, message, b)
}

// handleLogin 用户名密码登录。成功后为该浏览器创建独立 im.Client 并完成
// WS 连接 + 全量同步，随后浏览器通过 /ws 接收实时事件。
func handleLogin(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&in); err != nil || in.Username == "" {
		httpError(w, http.StatusBadRequest, "参数不完整")
		return
	}
	if old := sessions.get(r); old != nil {
		// 同一浏览器重复登录：先清理旧会话
		shutdownSession(old)
	}

	cfg := im.Config{
		AppKey:     *flagAppKey,
		APIURL:     *flagAPI,
		DeviceName: "go-demo",
		Platform:   "web",
		DeviceID:   fmt.Sprintf("go-demo-%08x", rand.Uint64()),
		Logger:     sdkLogger(),
	}
	c, err := im.New(cfg)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": errText(err)})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	if _, err := c.Login(ctx, "password", in.Username, in.Password); err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": errText(err)})
		return
	}
	connCtx, connCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer connCancel()
	if err := c.ConnectWait(connCtx); err != nil {
		log.Printf("[%s] 长连接连接失败（仍可 REST 使用）: %v", in.Username, err)
	}
	syncCtx, syncCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer syncCancel()
	if _, err := c.SyncAll(syncCtx); err != nil {
		log.Printf("[%s] 全量同步失败: %v", in.Username, err)
	}

	s := &session{
		id:      randToken(),
		c:       c,
		friends: map[string]im.Friend{},
		groups:  map[string]im.GroupInfo{},
	}
	s.refreshFriends()
	s.refreshGroups()
	sessions.put(s)

	// 订阅好友在线状态
	if fs, err := c.Friends(context.Background()); err == nil && len(fs) > 0 {
		names := make([]string, 0, len(fs))
		for _, f := range fs {
			names = append(names, f.Username)
		}
		_ = c.SubscribePresence(context.Background(), names)
	}

	http.SetCookie(w, &http.Cookie{
		Name: cookieName, Value: s.id, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode,
	})
	me, _ := c.Me(context.Background())
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "me": me})
}

func handleLogout(w http.ResponseWriter, r *http.Request) {
	if s := sessions.get(r); s != nil {
		shutdownSession(s)
	}
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: "", Path: "/", MaxAge: -1})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// shutdownSession 主动下线：断开浏览器桥、登出 SDK、移除会话。
func shutdownSession(s *session) {
	s.mu.Lock()
	br := s.br
	s.dead = true
	s.mu.Unlock()
	if br != nil {
		br.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.c.Logout(ctx)
	sessions.drop(s.id)
}

// handleUpload 浏览器 → 后端 multipart 上传，后端经 SDK 直传对象存储。
func handleUpload(w http.ResponseWriter, r *http.Request) {
	s := sessions.get(r)
	if s == nil {
		httpError(w, http.StatusUnauthorized, "未登录")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 32<<20)
	file, hdr, err := r.FormFile("file")
	if err != nil {
		httpError(w, http.StatusBadRequest, "读取上传内容失败: "+err.Error())
		return
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		httpError(w, http.StatusBadRequest, "读取上传内容失败: "+err.Error())
		return
	}
	ct := hdr.Header.Get("Content-Type")
	if ct == "" || ct == "application/octet-stream" {
		ct = http.DetectContentType(data)
	}
	in := im.UploadInput{Name: hdr.Filename, ContentType: ct, Data: data}
	if strings.HasPrefix(ct, "image/") {
		if cfg, _, err := image.DecodeConfig(bytes.NewReader(data)); err == nil {
			in.Width, in.Height = int64(cfg.Width), int64(cfg.Height)
		}
	}
	info, err := s.c.Upload(context.Background(), in)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": errText(err)})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"file_info": info})
}

// handleMedia 媒体下载代理：把签名直链的服务端下载流式回给浏览器，
// 避免浏览器直连对象存储（域名/签名均由 SDK 处理）。
func handleMedia(w http.ResponseWriter, r *http.Request) {
	s := sessions.get(r)
	if s == nil {
		httpError(w, http.StatusUnauthorized, "未登录")
		return
	}
	u := r.URL.Query().Get("u")
	if u == "" {
		httpError(w, http.StatusBadRequest, "缺少 u")
		return
	}
	raw, err := base64.RawURLEncoding.DecodeString(u)
	if err != nil {
		httpError(w, http.StatusBadRequest, "u 编码非法")
		return
	}
	data, err := s.c.Download(r.Context(), string(raw))
	if err != nil {
		httpError(w, http.StatusBadGateway, errText(err))
		return
	}
	ct := r.URL.Query().Get("ct")
	if ct == "" {
		ct = http.DetectContentType(data)
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Cache-Control", "private, max-age=3600")
	_, _ = w.Write(data)
}

func mediaSrc(u, ct string) string {
	if u == "" {
		return ""
	}
	return "/api/media?u=" + base64.RawURLEncoding.EncodeToString([]byte(u)) +
		"&ct=" + urlQueryEscape(ct)
}

func urlQueryEscape(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.', r == '~':
			b.WriteRune(r)
		default:
			for _, by := range []byte(string(r)) {
				fmt.Fprintf(&b, "%%%02X", by)
			}
		}
	}
	return b.String()
}

func errText(err error) string {
	var ae *im.APIError
	if errors.As(err, &ae) {
		return fmt.Sprintf("%s（code=%s）", ae.Message, ae.Code)
	}
	return err.Error()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}

func randToken() string {
	b := make([]byte, 18)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}

// ---------------------------------------------------------------------------
// 视图序列化

type convView struct {
	ID           string  `json:"id"`
	Type         string  `json:"type"`
	Peer         string  `json:"peer,omitempty"`
	GroupID      string  `json:"group_id,omitempty"`
	Name         string  `json:"name"`
	Unread       int64   `json:"unread"`
	Pinned       bool    `json:"pinned"`
	MarkedUnread bool    `json:"marked_unread"`
	Hidden       bool    `json:"hidden"`
	MaxSeq       int64   `json:"max_seq"`
	LastSeq      int64   `json:"last_seq"`
	LastPreview  string  `json:"last_preview"`
	LastAt       string  `json:"last_at"`
	LastSender   string  `json:"last_sender"`
	PeerReadSeq  int64   `json:"peer_read_seq"`
	Membership   *string `json:"membership"`
}

// convViews 从 SDK Store 组装会话列表（置顶优先，其余按最后消息时间倒序）。
func (s *session) convViews() []convView {
	out := []convView{}
	self := s.c.Store().Self()
	for _, st := range s.c.Store().Conversations() {
		v := convView{
			ID: st.ConversationID, Type: st.ConversationType,
			Peer: st.Peer, GroupID: st.GroupID,
			Unread: st.UnreadCount, Pinned: st.PinnedAt != "",
			MarkedUnread: st.MarkedUnread, Hidden: st.Hidden,
			MaxSeq: st.MaxSeq, LastSeq: st.MaxSeq,
			LastAt: st.LastMessageAt, PeerReadSeq: st.PeerReadSeq,
			Membership: st.Membership,
		}
		if v.Hidden {
			continue // 已删除的会话不进列表（清空历史的会话仍显示）
		}
		switch st.ConversationType {
		case "group":
			v.Name = "群 " + st.GroupID
			if g := s.groups[st.GroupID]; g.GroupID != "" && g.Name != "" {
				v.Name = g.Name
			}
		default:
			v.Name = st.Peer
			if f := s.friends[st.Peer]; f.Username != "" {
				if f.Remark != "" {
					v.Name = f.Remark
				} else if f.Nickname != "" {
					v.Name = f.Nickname
				}
			}
		}
		if msgs := s.c.Store().LastMessages(st.ConversationID, 1); len(msgs) > 0 {
			m := msgs[0]
			v.LastSeq = m.Seq
			v.LastAt = m.CreatedAt
			v.LastPreview = previewOf(m)
			if m.Sender == self {
				v.LastSender = "self"
			} else if m.SenderType == "system" {
				v.LastSender = "system"
			} else {
				v.LastSender = m.Sender
			}
		}
		out = append(out, v)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Pinned != out[j].Pinned {
			return out[i].Pinned
		}
		return out[i].LastAt > out[j].LastAt
	})
	return out
}

// previewOf 会话列表里的最后一条消息预览文案。
func previewOf(m *im.Message) string {
	if m.Recalled != nil {
		return "撤回了一条消息"
	}
	switch m.Type {
	case "text":
		return m.TextBody()
	case "image":
		return "[图片]"
	case "file":
		return "[文件]"
	case "voice":
		return "[语音]"
	case "video":
		return "[视频]"
	case "tip":
		return m.TextBody()
	default:
		return "[自定义消息]"
	}
}

// ---------------------------------------------------------------------------
// SDK 事件 → 浏览器推送

func (s *session) push(event string, data any) {
	s.mu.Lock()
	br := s.br
	s.mu.Unlock()
	if br == nil {
		return
	}
	payload, err := json.Marshal(map[string]any{"type": "push", "event": event, "data": data})
	if err != nil {
		return
	}
	_ = br.WriteMessage(payload)
}

func (s *session) pushConvs()     { s.push("convs", s.convViews()) }
func (s *session) pushFriends()   { s.push("friends", s.friendList()) }
func (s *session) pushBlacklist() { s.push("blacklist", s.blacklistList()) }

func (s *session) friendList() []im.Friend {
	out := make([]im.Friend, 0, len(s.friends))
	for _, f := range s.friends {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Username < out[j].Username })
	return out
}

func (s *session) blacklistList() []string {
	bl, err := s.c.Blacklist(context.Background())
	if err != nil {
		return []string{}
	}
	return bl
}

func (s *session) refreshFriends() {
	fs, err := s.c.Friends(context.Background())
	s.mu.Lock()
	if err == nil {
		s.friends = map[string]im.Friend{}
		for _, f := range fs {
			s.friends[f.Username] = f
		}
	}
	s.mu.Unlock()
}

func (s *session) refreshGroups() {
	gs, err := s.c.MyGroups(context.Background())
	s.mu.Lock()
	if err == nil {
		s.groups = map[string]im.GroupInfo{}
		for _, g := range gs {
			s.groups[g.GroupID] = g
		}
	}
	s.mu.Unlock()
}

// resyncConvs 后台重跑一次全量同步（新群、好友通过等新会话由此落地），完成后推送最新会话列表。
func (s *session) resyncConvs() {
	s.mu.Lock()
	busy := s.syncing
	s.syncing = true
	s.mu.Unlock()
	if busy {
		return
	}
	go func() {
		defer func() {
			s.mu.Lock()
			s.syncing = false
			s.mu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := s.c.SyncAll(ctx); err == nil && !s.isDead() {
			s.pushConvs()
		}
	}()
}

func (s *session) requestsPayload() map[string]any {
	received, _ := s.c.FriendRequests(context.Background(), "received")
	sent, _ := s.c.FriendRequests(context.Background(), "sent")
	unread, _ := s.c.FriendRequestUnreadCount(context.Background())
	return map[string]any{"received": received, "sent": sent, "unread": unread}
}

func (s *session) pushRequests() { s.push("requests", s.requestsPayload()) }

// wireEvents 把 SDK 事件/连接状态桥接到浏览器。
func (s *session) wireEvents() {
	s.c.OnEvent(func(e im.Event) {
		if s.isDead() {
			return
		}
		switch e.Name {
		case im.EventMessageReceived:
			var m im.Message
			if json.Unmarshal(e.Data, &m) == nil {
				s.push("msgs", map[string]any{"conversation_id": m.ConversationID, "messages": []im.Message{m}})
				s.pushConvs()
			}
		case im.EventMessageChanged:
			// message.changed 只带变化字段（不带整条消息），以补丁形式转给浏览器。
			var d im.MessageChangedData
			if json.Unmarshal(e.Data, &d) == nil {
				s.push("msgs_changed", d)
			}
		case im.EventMessageDeleted:
			var d im.MessageDeletedData
			if json.Unmarshal(e.Data, &d) == nil {
				s.push("msgs_deleted", d)
				s.pushConvs()
			}
		case im.EventConversationChanged, im.EventReadBy:
			s.pushConvs()
		case im.EventSyncHint:
			// 在线推送可能被服务端丢弃，重跑一次全量同步再推列表。
			s.resyncConvs()
		case im.EventPeerRead:
			var d im.PeerReadData
			if json.Unmarshal(e.Data, &d) == nil {
				s.push("peer_read", d)
			}
			s.pushConvs()
		case im.EventTyping:
			var d im.TypingData
			if json.Unmarshal(e.Data, &d) == nil {
				s.push("typing", d)
			}
		case im.EventFriendAdded, im.EventFriendUpdated, im.EventFriendRemoved:
			s.refreshFriends()
			s.pushFriends()
			s.pushConvs()
		case im.EventFriendRequestReceived, im.EventFriendRequestDeclined, im.EventFriendRequestRead:
			s.pushRequests()
		case im.EventBlacklistChanged:
			s.push("blacklist", s.blacklistList())
		case im.EventGroupCreated, im.EventGroupDismissed, im.EventGroupInfoChanged,
			im.EventGroupMembersAdded, im.EventGroupMembersRemoved, im.EventGroupMembersUpdated:
			s.refreshGroups()
			s.push("groups", s.groupList())
			// 新群/群变更需要等 conversations/sync 落地才有会话视图。
			s.resyncConvs()
		case im.EventUserProfileChanged:
			if me, err := s.c.Me(context.Background()); err == nil {
				s.push("profile", me)
			}
		case im.EventUserSessionsRevoked, im.EventUserReloginRequired, im.EventAuthExpired:
			go s.checkKicked()
		case im.EventPresenceChanged:
			var items []im.PresenceItem
			if json.Unmarshal(e.Data, &items) == nil {
				s.push("presence", items)
			} else {
				s.push("presence_raw", json.RawMessage(e.Data))
			}
		}
	})

	s.c.OnState(func(st im.ConnectionState) {
		if s.isDead() {
			return
		}
		s.push("conn", map[string]any{"state": string(st)})
		if st == im.StateDisconnected {
			go s.checkKicked()
		}
	})

	// SDK 在登录时已完成连接，状态回调不会再触发：浏览器接入时先补推当前状态。
	s.push("conn", map[string]any{"state": string(s.c.State())})
}

// checkKicked 断线后判定是否被踢/会话失效：SDK 对 session_revoked、
// device_mismatch 等关闭码不会重连；若 2.5s 后仍处断线且 REST 也 401，
// 判定为会话失效，推送 kicked 让浏览器回到登录页。
func (s *session) checkKicked() {
	time.Sleep(2500 * time.Millisecond)
	if s.isDead() || s.c.State() != im.StateDisconnected {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if _, err := s.c.Me(ctx); err != nil {
		s.mu.Lock()
		first := !s.kickFcm
		s.kickFcm = true
		s.dead = true
		s.mu.Unlock()
		// IM 会话已失效：同步移除浏览器 cookie 会话，避免刷新后仍进入僵尸主界面。
		sessions.drop(s.id)
		if first {
			s.push("kicked", map[string]any{"reason": errText(err)})
		}
	}
}

func (s *session) isDead() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dead
}

// ---------------------------------------------------------------------------
// WebSocket 桥

func handleWS(w http.ResponseWriter, r *http.Request) {
	s := sessions.get(r)
	// 会话可能仍挂在表里但已判定失效（被踢/吊销）：拒绝接入，浏览器回到登录页。
	if s == nil || s.isDead() {
		httpError(w, http.StatusUnauthorized, "未登录")
		return
	}
	br, err := acceptBridge(w, r)
	if err != nil {
		log.Printf("桥接升级失败: %v", err)
		return
	}
	s.mu.Lock()
	old := s.br
	s.br = br
	s.mu.Unlock()
	if old != nil {
		old.Close()
	}
	log.Printf("[%s] 浏览器已接入", s.c.CurrentUser())
	defer func() {
		s.mu.Lock()
		if s.br == br {
			s.br = nil
		}
		s.mu.Unlock()
		br.Close()
		log.Printf("[%s] 浏览器已断开", s.c.CurrentUser())
	}()

	s.wireEvents()
	s.bootstrap()

	// 读取浏览器动作
	for {
		payload, err := br.ReadMessage()
		if err != nil {
			return
		}
		var req struct {
			ID     uint64          `json:"id"`
			Action string          `json:"action"`
			Params json.RawMessage `json:"params"`
		}
		if json.Unmarshal(payload, &req) != nil || req.Action == "" {
			continue
		}
		if s.isDead() {
			s.reply(br, req.ID, nil, errors.New("会话已失效，请重新登录"))
			continue
		}
		data, aerr := s.dispatch(req.Action, req.Params)
		s.reply(br, req.ID, data, aerr)
	}
}

// dispatch 查表执行浏览器动作。
func (s *session) dispatch(action string, p json.RawMessage) (any, error) {
	fn, ok := actions[action]
	if !ok {
		return nil, fmt.Errorf("未知操作: %s", action)
	}
	return fn(s, p)
}

func (s *session) reply(br *bridgeConn, id uint64, data any, err error) {
	resp := map[string]any{"type": "reply", "id": id, "ok": err == nil}
	if err != nil {
		resp["error"] = errText(err)
	} else {
		resp["data"] = data
	}
	payload, _ := json.Marshal(resp)
	_ = br.WriteMessage(payload)
}

// bootstrap 登录快照：本人资料 + 各列表数据。
func (s *session) bootstrap() {
	me, _ := s.c.Me(context.Background())
	presence := []im.PresenceItem{}
	if names := friendNames(s.friendList()); len(names) > 0 {
		if items, err := s.c.QueryPresence(context.Background(), names); err == nil {
			presence = items
		}
	}
	s.push("bootstrap", map[string]any{
		"me":            me,
		"conversations": s.convViews(),
		"friends":       s.friendList(),
		"requests":      s.requestsPayload(),
		"groups":        s.groupList(),
		"invitations":   s.invitationList(),
		"blacklist":     s.blacklistList(),
		"presence":      presence,
		"self_session":  s.selfSessionID(),
	})
}

func (s *session) selfSessionID() string {
	return s.c.SessionID()
}

func (s *session) groupList() []im.GroupInfo {
	out := make([]im.GroupInfo, 0, len(s.groups))
	for _, g := range s.groups {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GroupID < out[j].GroupID })
	return out
}

func (s *session) invitationList() []im.GroupInvitation {
	inv, err := s.c.GroupInvitations(context.Background())
	if err != nil {
		return []im.GroupInvitation{}
	}
	return inv
}

func friendNames(fs []im.Friend) []string {
	out := make([]string, 0, len(fs))
	for _, f := range fs {
		out = append(out, f.Username)
	}
	return out
}

// avatarColor 由用户名派生的稳定头像底色（前端一致使用）。
func avatarColor(name string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return fmt.Sprintf("#%06x", h.Sum32()%0xffffff)
}

// ---------------------------------------------------------------------------
// 浏览器动作分发

type actionFunc func(s *session, p json.RawMessage) (any, error)

var actions = map[string]actionFunc{}

func init() {
	// ---- 会话 ----
	actions["open_conv"] = actOpenConv
	actions["open_peer"] = actOpenPeer
	actions["history"] = actHistory
	actions["send_text"] = actSendText
	actions["send_media"] = actSendMedia
	actions["recall"] = actRecall
	actions["delete_msgs"] = actDeleteMsgs
	actions["react"] = actReact
	actions["read"] = actRead
	actions["typing"] = actTyping
	actions["pin_conv"] = actPinConv
	actions["delete_conv"] = actDeleteConv
	actions["clear_conv"] = actClearConv
	// ---- 好友 ----
	actions["friends"] = noopList(func(s *session) any { return s.friendList() })
	actions["requests"] = func(s *session, _ json.RawMessage) (any, error) { return s.requestsPayload(), nil }
	actions["lookup_user"] = actLookupUser
	actions["add_friend"] = actAddFriend
	actions["accept_friend"] = actAcceptFriend
	actions["decline_friend"] = actDeclineFriend
	actions["remove_friend"] = actRemoveFriend
	actions["remark"] = actRemark
	actions["block"] = actBlock
	actions["unblock"] = actUnblock
	actions["blacklist"] = noopList(func(s *session) any { return s.blacklistList() })
	// ---- 群 ----
	actions["groups"] = noopList(func(s *session) any { return s.groupList() })
	actions["create_group"] = actCreateGroup
	actions["invite_group"] = actInviteGroup
	actions["leave_group"] = actLeaveGroup
	actions["rename_group"] = actRenameGroup
	actions["group_members"] = actGroupMembers
	actions["online_count"] = actOnlineCount
	actions["invitations"] = noopList(func(s *session) any { return s.invitationList() })
	actions["accept_invitation"] = actAcceptInvitation
	actions["decline_invitation"] = actDeclineInvitation
	// ---- 我的 ----
	actions["update_profile"] = actUpdateProfile
	actions["set_password"] = actSetPassword
	actions["sessions"] = actSessions
	actions["revoke_session"] = actRevokeSession
	actions["logout"] = actLogout
}

func noopList(fn func(*session) any) actionFunc {
	return func(s *session, _ json.RawMessage) (any, error) { return fn(s), nil }
}

var errBadParams = errors.New("参数不完整")

func param(p json.RawMessage, key string) string {
	var m map[string]any
	if json.Unmarshal(p, &m) != nil {
		return ""
	}
	switch v := m[key].(type) {
	case string:
		return v
	case float64:
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v))
		}
	}
	return ""
}

func paramInt(p json.RawMessage, key string) int64 {
	var m map[string]any
	if json.Unmarshal(p, &m) != nil {
		return 0
	}
	if v, ok := m[key].(float64); ok {
		return int64(v)
	}
	return 0
}

// targetOf 由会话 ID 推出发送目标（单聊→对端用户，群聊→群 ID）。
// "peer:用户名" 是尚未产生往来的伪会话，直接对用户发送。
func (s *session) targetOf(conversationID string) (im.Target, string, error) {
	if strings.HasPrefix(conversationID, "peer:") {
		peer := strings.TrimPrefix(conversationID, "peer:")
		if peer == "" {
			return im.Target{}, "", fmt.Errorf("伪会话缺少用户名")
		}
		return im.ToUser(peer), peer, nil
	}
	st := s.c.Store().Conversation(conversationID)
	if st == nil {
		return im.Target{}, "", fmt.Errorf("会话不存在或尚未同步: %s", conversationID)
	}
	if st.ConversationType == "group" {
		return im.ToGroup(st.GroupID), st.GroupID, nil
	}
	return im.ToUser(st.Peer), st.Peer, nil
}

func actOpenConv(s *session, p json.RawMessage) (any, error) {
	id := param(p, "id")
	if id == "" {
		return nil, errBadParams
	}
	st := s.c.Store().Conversation(id)
	if st == nil {
		return nil, fmt.Errorf("会话不存在: %s", id)
	}
	msgs := s.c.Store().LastMessages(id, 30)
	if len(msgs) == 0 && st.MaxSeq > 0 {
		fetched, err := s.c.FetchMessages(id, 0, 0, 30)
		if err == nil {
			msgs = fetched
		}
	}
	out := map[string]any{"conversation": st, "messages": msgs}
	if st.ConversationType == "group" {
		if g := s.groups[st.GroupID]; g.GroupID != "" {
			out["group"] = g
		}
	} else if f := s.friends[st.Peer]; f.Username != "" {
		out["friend"] = f
	}
	return out, nil
}

// actOpenPeer 与对端用户的单聊会话：已存在返回会话数据；尚无往来时返回
// conversation=null，前端用 "peer:用户名" 伪会话打开窗口，首条消息发出后
// 服务端建会话，再切到真实会话 ID。
func actOpenPeer(s *session, p json.RawMessage) (any, error) {
	peer := param(p, "username")
	if peer == "" {
		return nil, errBadParams
	}
	st, err := s.c.SingleConversation(context.Background(), peer)
	if err != nil {
		if im.IsCode(err, im.CodeNotFound) {
			return map[string]any{"conversation": nil, "peer": peer}, nil
		}
		return nil, err
	}
	s.pushConvs()
	return actOpenConv(s, jsonMarshal(map[string]string{"id": st.ConversationID}))
}

func jsonMarshal(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func actHistory(s *session, p json.RawMessage) (any, error) {
	id := param(p, "id")
	before := paramInt(p, "before_seq")
	if id == "" {
		return nil, errBadParams
	}
	msgs, err := s.c.History(id, before, 20)
	if err != nil {
		return nil, err
	}
	return map[string]any{"conversation_id": id, "messages": msgs}, nil
}

func actSendText(s *session, p json.RawMessage) (any, error) {
	id := param(p, "id")
	text := param(p, "text")
	reply := paramInt(p, "reply_seq")
	if id == "" || text == "" {
		return nil, errBadParams
	}
	target, _, err := s.targetOf(id)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var m *im.Message
	if reply > 0 {
		body, _ := json.Marshal(map[string]string{"text": text})
		m, err = s.c.SendReply(ctx, target, reply, "text", body, nil)
	} else {
		m, err = s.c.SendText(ctx, target, text, nil)
	}
	if err != nil {
		return nil, err
	}
	s.push("msgs", map[string]any{"conversation_id": m.ConversationID, "messages": []*im.Message{m}})
	s.pushConvs()
	return m, nil
}

func actSendMedia(s *session, p json.RawMessage) (any, error) {
	id := param(p, "id")
	msgType := param(p, "msg_type")
	if id == "" || (msgType != "image" && msgType != "file") {
		return nil, errBadParams
	}
	var raw struct {
		File im.FileInfo `json:"file"`
	}
	if err := json.Unmarshal(p, &raw); err != nil || raw.File.FileID == "" {
		return nil, errBadParams
	}
	target, _, err := s.targetOf(id)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	m, err := s.c.SendMedia(ctx, target, msgType, &raw.File, nil)
	if err != nil {
		return nil, err
	}
	s.push("msgs", map[string]any{"conversation_id": m.ConversationID, "messages": []*im.Message{m}})
	s.pushConvs()
	return m, nil
}

func actRecall(s *session, p json.RawMessage) (any, error) {
	id := param(p, "id")
	seq := paramInt(p, "seq")
	if id == "" || seq == 0 {
		return nil, errBadParams
	}
	return nil, s.c.Recall(context.Background(), id, seq)
}

func actDeleteMsgs(s *session, p json.RawMessage) (any, error) {
	id := param(p, "id")
	if id == "" {
		return nil, errBadParams
	}
	var m struct {
		Seqs []int64 `json:"seqs"`
	}
	_ = json.Unmarshal(p, &m)
	if len(m.Seqs) == 0 {
		return nil, errBadParams
	}
	return nil, s.c.DeleteMessages(context.Background(), id, m.Seqs)
}

func actReact(s *session, p json.RawMessage) (any, error) {
	id := param(p, "id")
	key := param(p, "key")
	seq := paramInt(p, "seq")
	remove := param(p, "remove") == "true"
	if id == "" || key == "" || seq == 0 {
		return nil, errBadParams
	}
	if remove {
		return nil, s.c.RemoveReaction(context.Background(), id, seq, key)
	}
	return nil, s.c.AddReaction(context.Background(), id, seq, key)
}

func actRead(s *session, p json.RawMessage) (any, error) {
	id := param(p, "id")
	seq := paramInt(p, "seq")
	if id == "" {
		return nil, errBadParams
	}
	if err := s.c.Read(context.Background(), id, seq); err != nil {
		return nil, err
	}
	s.pushConvs()
	return map[string]any{"ok": true}, nil
}

func actTyping(s *session, p json.RawMessage) (any, error) {
	var req struct {
		ID     string `json:"id"`
		ToUser string `json:"toUser"`
	}
	if err := json.Unmarshal(p, &req); err != nil {
		return nil, errBadParams
	}
	ctx := context.Background()
	if req.ToUser != "" {
		// 好友详情打开的伪会话（peer:用户名）没有真实会话 ID：改走 to_user 让服务端按单聊对象解析。
		_, _ = s.c.WSCall(ctx, "message.typing", map[string]any{"to_user": req.ToUser})
		return map[string]any{"ok": true}, nil
	}
	if req.ID == "" {
		return nil, errBadParams
	}
	s.c.Typing(ctx, req.ID)
	return map[string]any{"ok": true}, nil
}

func actPinConv(s *session, p json.RawMessage) (any, error) {
	id := param(p, "id")
	pinned := param(p, "pinned") == "true"
	if id == "" {
		return nil, errBadParams
	}
	if err := s.c.SetConversationPinned(context.Background(), id, pinned); err != nil {
		return nil, err
	}
	s.pushConvs()
	return map[string]any{"ok": true}, nil
}

func actDeleteConv(s *session, p json.RawMessage) (any, error) {
	id := param(p, "id")
	if id == "" {
		return nil, errBadParams
	}
	if err := s.c.DeleteConversation(context.Background(), id); err != nil {
		return nil, err
	}
	s.pushConvs()
	return map[string]any{"ok": true}, nil
}

func actClearConv(s *session, p json.RawMessage) (any, error) {
	id := param(p, "id")
	if id == "" {
		return nil, errBadParams
	}
	if err := s.c.ClearConversation(context.Background(), id); err != nil {
		return nil, err
	}
	s.pushConvs()
	return map[string]any{"ok": true}, nil
}

// ---- 好友 ----

func actLookupUser(s *session, p json.RawMessage) (any, error) {
	name := param(p, "username")
	if name == "" {
		return nil, errBadParams
	}
	return s.c.GetUser(context.Background(), name)
}

func actAddFriend(s *session, p json.RawMessage) (any, error) {
	name := param(p, "username")
	message := param(p, "message")
	if name == "" {
		return nil, errBadParams
	}
	if _, err := s.c.SendFriendRequest(context.Background(), name, message); err != nil {
		return nil, err
	}
	s.pushRequests()
	return map[string]any{"ok": true}, nil
}

func actAcceptFriend(s *session, p json.RawMessage) (any, error) {
	name := param(p, "username")
	if name == "" {
		return nil, errBadParams
	}
	if err := s.c.AcceptFriendRequest(context.Background(), name); err != nil {
		return nil, err
	}
	s.refreshFriends()
	s.pushFriends()
	s.pushConvs()
	s.pushRequests()
	return map[string]any{"ok": true}, nil
}

func actDeclineFriend(s *session, p json.RawMessage) (any, error) {
	name := param(p, "username")
	if name == "" {
		return nil, errBadParams
	}
	if err := s.c.DeclineFriendRequest(context.Background(), name); err != nil {
		return nil, err
	}
	s.pushRequests()
	return map[string]any{"ok": true}, nil
}

func actRemoveFriend(s *session, p json.RawMessage) (any, error) {
	name := param(p, "username")
	if name == "" {
		return nil, errBadParams
	}
	if err := s.c.RemoveFriend(context.Background(), name); err != nil {
		return nil, err
	}
	s.refreshFriends()
	s.pushFriends()
	s.pushConvs()
	return map[string]any{"ok": true}, nil
}

func actRemark(s *session, p json.RawMessage) (any, error) {
	name := param(p, "username")
	remark := param(p, "remark")
	if name == "" {
		return nil, errBadParams
	}
	if err := s.c.SetFriendRemark(context.Background(), name, remark, nil); err != nil {
		return nil, err
	}
	s.refreshFriends()
	s.pushFriends()
	s.pushConvs()
	return map[string]any{"ok": true}, nil
}

func actBlock(s *session, p json.RawMessage) (any, error) {
	name := param(p, "username")
	if name == "" {
		return nil, errBadParams
	}
	if err := s.c.BlockUser(context.Background(), name); err != nil {
		return nil, err
	}
	s.push("blacklist", s.blacklistList())
	return map[string]any{"ok": true}, nil
}

func actUnblock(s *session, p json.RawMessage) (any, error) {
	name := param(p, "username")
	if name == "" {
		return nil, errBadParams
	}
	if err := s.c.UnblockUser(context.Background(), name); err != nil {
		return nil, err
	}
	s.push("blacklist", s.blacklistList())
	return map[string]any{"ok": true}, nil
}

// ---- 群 ----

func actCreateGroup(s *session, p json.RawMessage) (any, error) {
	var in struct {
		Name    string   `json:"name"`
		Members []string `json:"members"`
	}
	if err := json.Unmarshal(p, &in); err != nil || in.Name == "" {
		return nil, errBadParams
	}
	g, err := s.c.CreateGroup(context.Background(), im.CreateGroupInput{Name: in.Name, Members: in.Members})
	if err != nil {
		return nil, err
	}
	s.refreshGroups()
	// MyGroups 可能尚未返回新群，用创建响应（名称最权威）补齐缓存，避免会话标题回退为「群 +ID」。
	if g.GroupID != "" {
		s.mu.Lock()
		s.groups[g.GroupID] = *g
		s.mu.Unlock()
	}
	s.push("groups", s.groupList())
	s.pushConvs()
	return g, nil
}

func actInviteGroup(s *session, p json.RawMessage) (any, error) {
	gid := param(p, "group_id")
	if gid == "" {
		return nil, errBadParams
	}
	var in struct {
		Users []string `json:"users"`
	}
	if err := json.Unmarshal(p, &in); err != nil || len(in.Users) == 0 {
		return nil, errBadParams
	}
	res, err := s.c.AddGroupMembers(context.Background(), gid, in.Users)
	if err != nil {
		return nil, err
	}
	s.refreshGroups()
	s.push("groups", s.groupList())
	return res, nil
}

func actLeaveGroup(s *session, p json.RawMessage) (any, error) {
	gid := param(p, "group_id")
	if gid == "" {
		return nil, errBadParams
	}
	if err := s.c.LeaveGroup(context.Background(), gid); err != nil {
		return nil, err
	}
	s.refreshGroups()
	s.push("groups", s.groupList())
	s.pushConvs()
	return map[string]any{"ok": true}, nil
}

func actRenameGroup(s *session, p json.RawMessage) (any, error) {
	gid := param(p, "id")
	name := param(p, "name")
	if gid == "" || name == "" {
		return nil, errBadParams
	}
	g, err := s.c.UpdateGroup(context.Background(), gid, &name, nil, nil)
	if err != nil {
		return nil, err
	}
	s.refreshGroups()
	s.push("groups", s.groupList())
	s.pushConvs()
	return g, nil
}

func actGroupMembers(s *session, p json.RawMessage) (any, error) {
	gid := param(p, "group_id")
	if gid == "" {
		return nil, errBadParams
	}
	return s.c.GroupMembers(context.Background(), gid)
}

func actOnlineCount(s *session, p json.RawMessage) (any, error) {
	gid := param(p, "group_id")
	if gid == "" {
		return nil, errBadParams
	}
	n, err := s.c.GroupOnlineCount(context.Background(), gid)
	if err != nil {
		return nil, err
	}
	return map[string]any{"online_count": n}, nil
}

func actAcceptInvitation(s *session, p json.RawMessage) (any, error) {
	iid := param(p, "invitation_id")
	if iid == "" {
		return nil, errBadParams
	}
	g, err := s.c.AcceptGroupInvitation(context.Background(), iid)
	if err != nil {
		return nil, err
	}
	s.refreshGroups()
	s.push("groups", s.groupList())
	s.push("invitations", s.invitationList())
	s.pushConvs()
	return g, nil
}

func actDeclineInvitation(s *session, p json.RawMessage) (any, error) {
	iid := param(p, "invitation_id")
	if iid == "" {
		return nil, errBadParams
	}
	if err := s.c.DeclineGroupInvitation(context.Background(), iid); err != nil {
		return nil, err
	}
	s.push("invitations", s.invitationList())
	return map[string]any{"ok": true}, nil
}

// ---- 我的 ----

func actUpdateProfile(s *session, p json.RawMessage) (any, error) {
	nickname := param(p, "nickname")
	if nickname == "" {
		return nil, errBadParams
	}
	me, err := s.c.UpdateProfile(context.Background(), nickname, "", nil)
	if err != nil {
		return nil, err
	}
	s.push("profile", me)
	return me, nil
}

func actSetPassword(s *session, p json.RawMessage) (any, error) {
	oldp := param(p, "old")
	newp := param(p, "new")
	if oldp == "" || newp == "" {
		return nil, errBadParams
	}
	if err := s.c.SetPassword(context.Background(), oldp, newp); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}

func actSessions(s *session, _ json.RawMessage) (any, error) {
	list, err := s.c.Sessions(context.Background())
	if err != nil {
		return nil, err
	}
	return map[string]any{"items": list, "current": s.c.SessionID()}, nil
}

func actRevokeSession(s *session, p json.RawMessage) (any, error) {
	sid := param(p, "session_id")
	if sid == "" {
		return nil, errBadParams
	}
	if sid == s.c.SessionID() {
		// 下线本机：吊销后 SDK 会话失效，桥推送 kicked
		if err := s.c.RevokeSession(context.Background(), sid); err != nil {
			return nil, err
		}
		go func() {
			time.Sleep(400 * time.Millisecond)
			s.checkKicked()
		}()
		return map[string]any{"ok": true, "self": true}, nil
	}
	if err := s.c.RevokeSession(context.Background(), sid); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}

func actLogout(s *session, _ json.RawMessage) (any, error) {
	go shutdownSession(s)
	return map[string]any{"ok": true}, nil
}
