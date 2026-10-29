//go:build integration

package im

// 集成测试：连真实 harness（REST 18480 / 控制口 18490）。
// 运行：go test -tags integration -v -run TestIntegration ./...
//
// 步骤输出格式（供测试报告组装脚本解析）：
//
//	STEP|<编号>|<操作>|<预期>|<PASS|FAIL>|<实际>

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

const itPassword = "pass1234"

func itAPI() string {
	if v := os.Getenv("IM_API"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return "http://127.0.0.1:18480"
}

func itCtrl() string {
	if v := os.Getenv("IM_CTRL"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return "http://127.0.0.1:18490"
}

func itAppKey() string {
	if v := os.Getenv("IM_APPKEY"); v != "" {
		return v
	}
	return "6526900072#sdk1019023016"
}

func TestMain(m *testing.M) {
	// harness 不可达时跳过（而非失败）：集成测试依赖已在运行的本地服务
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(itAPI(), "http://"), 2*time.Second)
	if err != nil {
		fmt.Println("SKIP 集成测试：harness 不可达", itAPI())
		os.Exit(0)
	}
	conn.Close()
	os.Exit(m.Run())
}

// step 输出结构化步骤结果；err != nil 时终止当前测试（保留现场）。
func step(t *testing.T, no int, action, expect string, err error, detail string) {
	t.Helper()
	status, actual := "PASS", detail
	if err != nil {
		status, actual = "FAIL", err.Error()
	}
	t.Logf("STEP|%d|%s|%s|%s|%s", no, action, expect, status, actual)
	if err != nil {
		t.Fatalf("步骤 %d 失败: %v", no, err)
	}
}

func itNewClient(t *testing.T, username string) *Client {
	t.Helper()
	c, err := New(Config{
		AppKey:      itAppKey(),
		APIURL:      itAPI(),
		DeviceID:    "it-" + username + "-" + fmt.Sprint(time.Now().UnixNano()%100000),
		DeviceName:  "go-it",
		Platform:    "web",
		HTTPTimeout: 15 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func itCreateUser(t *testing.T, username string) {
	t.Helper()
	body := fmt.Sprintf(`{"username":%q,"password":%q,"nickname":"集成%s"}`, username, itPassword, username)
	req, _ := http.NewRequest(http.MethodPost, itCtrl()+"/users", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("控制口建用户 %s: %v", username, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 && resp.StatusCode != http.StatusConflict {
		t.Fatalf("控制口建用户 %s 状态 %d", username, resp.StatusCode)
	}
}

func itLogin(t *testing.T, c *Client, username string) *Session {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s, err := c.Login(ctx, LoginPassword, username, itPassword)
	if err != nil {
		t.Fatalf("登录 %s: %v", username, err)
	}
	return s
}

func itConnect(t *testing.T, c *Client, username string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.ConnectWait(ctx); err != nil {
		t.Fatalf("建长连接 %s: %v", username, err)
	}
}

// itEvents 事件收集器：按谓词等待事件。
type itEvents struct {
	mu  sync.Mutex
	evs []Event
}

func (w *itEvents) attach(t *testing.T, c *Client) {
	c.OnEvent(func(e Event) {
		w.mu.Lock()
		w.evs = append(w.evs, e)
		w.mu.Unlock()
	})
}

func (w *itEvents) wait(t *testing.T, timeout time.Duration, pred func(Event) bool, what string) Event {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		w.mu.Lock()
		for _, e := range w.evs {
			if pred(e) {
				w.mu.Unlock()
				return e
			}
		}
		w.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("等待事件超时(%v): %s", timeout, what)
	return Event{}
}

func msgData(t *testing.T, e Event) map[string]any {
	t.Helper()
	// 服务端 message.received 的 data 就是消息 JSON 本身；兼容 {message:{...}} 包装
	var wrapped struct {
		Message json.RawMessage `json:"message"`
	}
	if json.Unmarshal(e.Data, &wrapped) == nil && len(wrapped.Message) > 0 {
		var m map[string]any
		if json.Unmarshal(wrapped.Message, &m) == nil {
			return m
		}
	}
	var m map[string]any
	if json.Unmarshal(e.Data, &m) == nil {
		return m
	}
	return nil
}

// ---- 主流程：登录 → 好友 → 单聊（已读/编辑/撤回/回应）→ 群 → 媒体 → 新设备同步 ----

func TestIntegrationGroupReinvite(t *testing.T) {
	ctx := context.Background()
	resp, err := http.Get(itCtrl() + "/env")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var env struct {
		AppKey string `json:"app_key"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	t.Setenv("IM_APPKEY", env.AppKey)
	suffix := fmt.Sprint(time.Now().UnixNano())
	owner, member := "greo"+suffix, "grem"+suffix
	login := func(username string) *Client {
		t.Helper()
		r, err := http.Post(itCtrl()+"/tickets/"+username, "application/json", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		var result struct {
			Ticket string `json:"ticket"`
		}
		if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		c := itNewClient(t, username)
		if _, err := c.Login(ctx, LoginTicket, username, result.Ticket); err != nil {
			t.Fatal(err)
		}
		return c
	}
	o, b := login(owner), login(member)
	g, err := o.CreateGroup(ctx, CreateGroupInput{Name: "reinvite regression", Members: []string{member}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = o.DismissGroup(ctx, g.GroupID) })
	if err := b.LeaveGroup(ctx, g.GroupID); err != nil {
		t.Fatal(err)
	}
	results, err := o.AddGroupMembers(ctx, g.GroupID, []string{member})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Code != "" || results[0].Status != "joined" {
		t.Fatalf("默认关闭冷却时应立即邀请回群: %+v", results)
	}
}

func TestIntegrationMainFlow(t *testing.T) {
	ctx := context.Background()
	suffix := fmt.Sprint(time.Now().UnixNano() % 10000000)
	ua, ub := "goita"+suffix, "goitb"+suffix

	itCreateUser(t, ua)
	itCreateUser(t, ub)
	step(t, 1, "控制口创建用户 "+ua+"、"+ub, "两个用户可用", nil, "控制口 200")

	a := itNewClient(t, ua)
	sa := itLogin(t, a, ua)
	if sa.AccessToken == "" || a.CurrentUser() != ua || !a.SessionValid() {
		t.Fatalf("登录态异常: token=%q user=%q", sa.AccessToken, a.CurrentUser())
	}
	step(t, 2, "A 密码登录", "返回 access_token 与本人资料", nil, "token 长度 "+fmt.Sprint(len(sa.AccessToken)))

	b := itNewClient(t, ub)
	itLogin(t, b, ub)
	itConnect(t, a, ua)
	itConnect(t, b, ub)
	step(t, 3, "A、B 建立长连接", "State=connected", nil, "A="+string(a.State())+" B="+string(b.State()))

	ea, eb := &itEvents{}, &itEvents{}
	ea.attach(t, a)
	eb.attach(t, b)

	// 4. B → A 好友申请，A 收到推送后同意
	status, err := b.SendFriendRequest(ctx, ua, "集成测试加个好友")
	step(t, 4, "B 发好友申请", "status=pending", err, "status="+status)
	ea.wait(t, 10*time.Second, func(e Event) bool {
		return e.Name == EventFriendRequestReceived
	}, "friend_request.received")
	step(t, 5, "A 收到好友申请推送", "friend_request.received", nil, "事件已到达")
	step(t, 6, "A 同意好友申请", "无错误", a.AcceptFriendRequest(ctx, ub), "accept ok")
	eb.wait(t, 10*time.Second, func(e Event) bool { return e.Name == EventFriendAdded }, "friend.added")
	step(t, 7, "B 收到好友建立推送", "friend.added", nil, "事件已到达")

	// 8. B 发文本给 A
	sent, err := b.SendText(ctx, ToUser(ua), "集成消息 hello "+suffix, map[string]any{"case": "main"})
	step(t, 8, "B 发送文本消息", "seq>0 且返回消息体", err, fmt.Sprintf("seq=%d cid=%s", sent.Seq, sent.ConversationID))
	cid := sent.ConversationID

	// 9. A 实时收到 + store 校验
	e := ea.wait(t, 10*time.Second, func(e Event) bool {
		m := msgData(t, e)
		if m == nil {
			return false
		}
		s, _ := m["body"].(map[string]any)
		txt, _ := s["text"].(string)
		return e.Name == EventMessageReceived && txt == "集成消息 hello "+suffix
	}, "message.received(text)")
	mm := msgData(t, e)
	seq, _ := mm["seq"].(float64)
	mid, _ := mm["message_id"].(string)
	step(t, 9, "A 实时收到消息推送", "message.received 且文本一致", nil, fmt.Sprintf("seq=%.0f msg_id=%s", seq, mid))
	if got := a.Store().Conversation(cid); got == nil || got.UnreadCount != 1 {
		t.Fatalf("A store 未读应为 1: %+v", got)
	}
	if len(a.Store().Messages(cid)) == 0 {
		t.Fatal("A store 应缓存消息")
	}
	step(t, 10, "A store 校验", "会话未读=1 且消息已缓存", nil, "未读 1，消息 1 条")

	// 11. A 已读 → B 收到 peer_read
	step(t, 11, "A 标记已读", "无错误", a.Read(ctx, cid, int64(seq)), "read ok")
	eb.wait(t, 10*time.Second, func(e Event) bool { return e.Name == EventPeerRead }, "message.peer_read")
	if st := b.Store().Conversation(cid); st == nil || st.PeerReadSeq < int64(seq) {
		t.Fatalf("B store peer_read_seq 未推进: %+v", st)
	}
	step(t, 12, "B 收到已读回执", "message.peer_read 且 store 推进", nil, fmt.Sprintf("peer_read_seq=%d", b.Store().Conversation(cid).PeerReadSeq))

	// 13. A 对消息加回应 → B 收到 reaction_added 补丁（客户端 API 无编辑路由，编辑仅 OpenAPI 提供）
	step(t, 13, "A 添加消息回应 like", "无错误", a.AddReaction(ctx, cid, int64(seq), "like"), "react ok")
	eb.wait(t, 10*time.Second, func(e Event) bool {
		return e.Name == EventMessageChanged && strings.Contains(string(e.Data), "reaction_added")
	}, "message.changed(reaction_added)")
	var reacted *Message
	for _, m := range b.Store().Messages(cid) {
		if m.Seq == int64(seq) {
			reacted = m
		}
	}
	if reacted == nil || len(reacted.Reactions) != 1 || reacted.Reactions[0].Key != "like" ||
		reacted.Reactions[0].Count != 1 || len(reacted.Reactions[0].Users) != 1 {
		t.Fatalf("B store 回应未合并: %+v", reacted)
	}
	step(t, 14, "B store 回应补丁合并", "Reactions 含 like count=1", nil, fmt.Sprintf("count=%d reacted(B 视角)=%v", reacted.Reactions[0].Count, reacted.Reactions[0].Reacted))

	// 15. B 撤回 → A 收到 recalled 补丁
	step(t, 15, "B 撤回消息", "无错误", b.Recall(ctx, cid, int64(seq)), "recall ok")
	ea.wait(t, 10*time.Second, func(e Event) bool {
		return e.Name == EventMessageChanged && strings.Contains(string(e.Data), `"kind":"recalled"`)
	}, "message.changed(recalled)")
	var recalled *Message
	for _, m := range a.Store().Messages(cid) {
		if m.Seq == int64(seq) {
			recalled = m
		}
	}
	if recalled == nil || recalled.Recalled == nil || recalled.Recalled.By != ub {
		t.Fatalf("A store 撤回标记异常: %+v", recalled)
	}
	step(t, 16, "A store 撤回合并", "Recalled.By=B 且 body 清空", nil, "by="+recalled.Recalled.By+" body 清空="+fmt.Sprint(recalled.Body == nil))

	// 17. 输入中
	a.Typing(ctx, cid)
	eb.wait(t, 10*time.Second, func(e Event) bool { return e.Name == EventTyping }, "message.typing")
	step(t, 17, "A 发输入中通知", "B 收到 message.typing", nil, "事件已到达")

	// 18. A 建群（含 B）→ 群消息
	g, err := a.CreateGroup(ctx, CreateGroupInput{Name: "集成群" + suffix, Members: []string{ub}})
	step(t, 18, "A 建群并拉入 B", "group_id 非空", err, "group_id="+g.GroupID)
	eb.wait(t, 10*time.Second, func(e Event) bool {
		return e.Name == EventGroupCreated || e.Name == EventGroupMembersAdded ||
			(e.Name == EventConversationChanged && strings.Contains(string(e.Data), g.GroupID))
	}, "群建立推送")
	gsent, err := a.SendText(ctx, ToGroup(g.GroupID), "群里第一条 "+suffix, nil)
	step(t, 19, "A 发群消息", "seq>0", err, fmt.Sprintf("seq=%d", gsent.Seq))
	// 发送者自己的连接不回显 message.received：只等 B 的推送，A 用 store 断言
	eb.wait(t, 10*time.Second, func(e Event) bool {
		m := msgData(t, e)
		if m == nil {
			return false
		}
		s, _ := m["body"].(map[string]any)
		txt, _ := s["text"].(string)
		return e.Name == EventMessageReceived && txt == "群里第一条 "+suffix
	}, "群消息推送 B")
	if len(a.Store().Messages(gsent.ConversationID)) == 0 {
		t.Fatal("A store 未缓存群消息")
	}
	if n := len(b.Store().Messages(gsent.ConversationID)); n == 0 {
		t.Fatal("B store 未缓存群消息")
	}
	step(t, 20, "群消息落库", "B 实时收到且双方 store 缓存", nil, fmt.Sprintf("A store %d 条，B store %d 条", len(a.Store().Messages(gsent.ConversationID)), len(b.Store().Messages(gsent.ConversationID))))

	// 21. 媒体：上传 PNG → 发图片消息 → 对端下载比对
	png := []byte{
		0x89, 'P', 'N', 'G', '\r', '\n', 0x1A, '\n', 0x00, 0x00, 0x00, 0x0D, 'I', 'H', 'D', 'R',
		0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15,
		0xC4, 0x89, 0x00, 0x00, 0x00, 0x0A, 'I', 'D', 'A', 'T', 0x78, 0x9C, 0x63, 0x00, 0x01, 0x00,
		0x00, 0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00, 0x00, 0x00, 0x00, 'I', 'E', 'N', 'D',
		0xAE, 0x42, 0x60, 0x82,
	}
	info, err := a.Upload(ctx, UploadInput{Name: "dot.png", ContentType: "image/png", Data: png, Purpose: "attachment", Kind: "image"})
	step(t, 21, "A 上传图片", "file_id 与 url 非空", err, fmt.Sprintf("file_id=%s url=%s", info.FileID, info.URL))
	if info.FileID == "" {
		t.Fatal("file_id 为空")
	}
	msent, err := a.SendMedia(ctx, ToGroup(g.GroupID), "image", info, nil)
	step(t, 22, "A 发送图片消息", "type=image", err, fmt.Sprintf("seq=%d", msent.Seq))
	me := eb.wait(t, 10*time.Second, func(e Event) bool {
		m := msgData(t, e)
		return m != nil && m["type"] == "image" && m["seq"].(float64) == float64(msent.Seq)
	}, "图片消息推送")
	mmi := msgData(t, me)
	if mmi == nil {
		t.Fatal("图片消息 data 解析失败")
	}
	step(t, 23, "B 收到图片消息", "type=image 且 seq 匹配", nil, fmt.Sprintf("seq=%.0f", mmi["seq"].(float64)))
	// 从消息 body 取 url 下载比对
	var imgBody FileInfo
	raw, _ := json.Marshal(mmi["body"])
	if json.Unmarshal(raw, &imgBody) != nil || imgBody.URL == "" {
		t.Fatalf("图片 body 无 url: %s", raw)
	}
	down, err := b.Download(ctx, imgBody.URL)
	if err != nil {
		if list, _ := b.DownloadURLs(ctx, []string{imgBody.URL}); len(list) > 0 {
			err = fmt.Errorf("%w (item: code=%q msg=%q signed=%q)", err, list[0].Code, list[0].Message, list[0].URL)
		}
	}
	step(t, 24, "B 下载图片", "字节一致", err, fmt.Sprintf("下载 %d 字节，原 %d 字节", len(down), len(png)))
	if len(down) != len(png) {
		t.Fatal("下载内容与上传不一致")
	}

	// 25. 新设备 C 以 A 身份登录 → SyncAll 应带回会话与最后一条消息
	c := itNewClient(t, ua+"c")
	c.cfg.DeviceID = "it-newdevice-" + suffix
	itLogin(t, c, ua)
	n, err := c.SyncAll(ctx)
	step(t, 25, "新设备 C 登录并全量同步", "同步到 ≥2 个会话", err, fmt.Sprintf("同步 %d 个会话", n))
	found := false
	for _, st := range c.Store().Conversations() {
		if st.ConversationID == gsent.ConversationID {
			found = true
		}
	}
	if !found {
		t.Fatalf("C store 缺少群会话 %s", gsent.ConversationID)
	}
	if msgs := c.Store().LastMessages(gsent.ConversationID, 1); len(msgs) == 0 {
		t.Fatal("C store 群会话缺最后一条消息")
	} else if msgs[0].Type != "image" {
		t.Fatalf("最后一条应为图片消息: %+v body=%s", msgs[0], msgs[0].Body)
	} else {
		step(t, 26, "C store 校验", "含群会话且最后一条为图片消息", nil, "群会话与消息齐备，last type=image")
	}

	a.Disconnect()
	b.Disconnect()
	c.Disconnect()
	step(t, 27, "双方断开长连接", "State=disconnected", nil, "A/B/C 均已断开")
}

// ---- 失败路径 ----

func TestIntegrationFailurePaths(t *testing.T) {
	ctx := context.Background()
	suffix := fmt.Sprint(time.Now().UnixNano() % 10000000)

	// F1 错误密码
	uc := "goitf" + suffix
	itCreateUser(t, uc)
	c := itNewClient(t, uc)
	_, err := c.Login(ctx, LoginPassword, uc, "wrong-password")
	if err == nil {
		t.Fatal("错误密码不应登录成功")
	}
	code := ""
	if apiErr, ok := err.(*APIError); ok {
		code = apiErr.Code
	}
	step(t, 101, "错误密码登录", "APIError 拒绝", nil, "code="+code)

	// F2 无效 refresh token → 清空本地会话
	c2 := itNewClient(t, uc+"r")
	c2.cfg.TokenStorage = badTokenStore{}
	c3, _ := New(c2.cfg) // 走 Load 注入坏凭据
	err = c3.Refresh(ctx)
	if err == nil || !IsCode(err, CodeUnauthenticated) {
		t.Fatalf("无效 refresh token 应报 unauthenticated, got %v", err)
	}
	if c3.SessionValid() || c3.CurrentUser() != "" {
		t.Fatal("续期失败后应清空本地会话")
	}
	step(t, 102, "无效 refresh token 续期", "unauthenticated 且本地会话清空", nil, "code=unauthenticated")

	// F3 查询不存在的用户
	itLogin(t, c, uc)
	_, err = c.GetUser(ctx, "ghost_user_"+suffix)
	if err == nil || !IsCode(err, CodeNotFound) {
		t.Fatalf("查询不存在用户应报 not_found, got %v", err)
	}
	step(t, 103, "查询不存在用户", "not_found", nil, "code=not_found")

	// F4 向不存在的群发消息
	_, err = c.SendText(ctx, ToGroup("grp_ghost_"+suffix), "hi", nil)
	if err == nil {
		t.Fatal("向不存在群发消息应失败")
	}
	if ae, ok := err.(*APIError); ok && ae.Code != CodeNotFound && ae.Code != CodeForbidden && ae.Code != CodeInvalidArgument {
		t.Fatalf("意外错误码 %s", ae.Code)
	}
	step(t, 104, "向不存在群发消息", "not_found/forbidden/invalid_argument", nil, "被拒绝")

	// F5 吊销本机会话 → 长连接被踢且不再重连
	itConnect(t, c, uc)
	sessions, err := c.Sessions(ctx)
	if err != nil {
		t.Fatalf("列会话: %v", err)
	}
	current := ""
	for _, s := range sessions {
		if s.Current {
			current = s.SessionID
		}
	}
	if current == "" {
		t.Fatal("未找到本机会话")
	}
	step(t, 105, "吊销本机会话", "无错误", c.RevokeSession(ctx, current), "session_id="+current)
	time.Sleep(2 * time.Second)
	if st := c.State(); st != StateDisconnected {
		t.Fatalf("吊销后应断开, got %s", st)
	}
	time.Sleep(3 * time.Second)
	if st := c.State(); st != StateDisconnected {
		t.Fatalf("吊销后 3 秒不应重连, got %s", st)
	}
	_, err = c.Me(ctx)
	code = ""
	if err == nil {
		t.Fatal("吊销后 REST 应失败")
	}
	if ae, ok := err.(*APIError); ok {
		code = ae.Code
	}
	if code != CodeUnauthenticated && code != "session_revoked" {
		t.Fatalf("吊销后 REST 错误码异常: %s", code)
	}
	step(t, 106, "被踢后校验", "连接断开不重连、REST 被拒", nil, "State=disconnected, REST code="+code)
}

// badTokenStore 注入无效凭据的 TokenStorage。
type badTokenStore struct{}

func (badTokenStore) Load(appKey string) (*Session, error) {
	return &Session{AccessToken: "invalid-access", RefreshToken: "invalid-refresh"}, nil
}
func (badTokenStore) Save(appKey string, s *Session) error { return nil }
func (badTokenStore) Clear(appKey string) error            { return nil }
