package im

// Store 纯内存单测：会话排序、消息去重/删除、applyEvent 各事件分支（seq 三态、
// message.changed 平铺补丁合并、cleared_seq 清理、peer_read、typing 过期、事件去重）。

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// syncStub 拦 applyEvent 里的异步网络行为（fetchMissingAsync / lightSync），
// 记录命中的路径并返回最小合法响应，避免 goroutine 打真实网络。
type syncStub struct {
	mu   sync.Mutex
	hits map[string]int
	srv  *httptest.Server
}

func newSyncStub(t *testing.T) *syncStub {
	s := &syncStub{hits: map[string]int{}}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.hits[r.URL.Path]++
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/client/v1/conversations/recent":
			_, _ = w.Write([]byte(`{"version":1,"sync_epoch":"e1","pinned":[],"items":[]}`))
		case r.URL.Path == "/client/v1/conversations/group-digest":
			_, _ = w.Write([]byte(`{"items":[]}`))
		case r.URL.Path == "/client/v1/conversations/sync":
			_, _ = w.Write([]byte(`{"items":[],"has_more":false,"next_version":1}`))
		default:
			_, _ = w.Write([]byte(`{"items":[]}`))
		}
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *syncStub) hitCount(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[path]
}

// waitForStub 等待指定路径命中 n 次（异步 goroutine 完成的信号）。
func (s *syncStub) waitFor(t *testing.T, path string, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s.hitCount(path) >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待 %s 命中 %d 次超时（实际 %d 次）", path, n, s.hitCount(path))
}

func newTestClient(t *testing.T) (*Client, *syncStub) {
	t.Helper()
	stub := newSyncStub(t)
	c, err := New(Config{AppKey: "org#testapp", APIURL: stub.srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c, stub
}

func rawEvent(name, id string, data any) *Event {
	b, _ := json.Marshal(data)
	return &Event{Name: name, ID: id, Data: b}
}

// seedMessage 用 message.received 事件灌入一条消息（作为 changed 类测试的底料）。
func seedMessage(t *testing.T, c *Client, convID string, seq int64, body string) *Message {
	t.Helper()
	m := map[string]any{
		"conversation_id": convID, "conversation_type": "single", "seq": seq,
		"message_id": fmt.Sprintf("m%d", seq), "sender": "alice", "recipient": "bob",
		"type": "text", "body": map[string]string{"text": body},
		"created_at": time.Now().UTC().Format(time.RFC3339), "message_version": 1,
	}
	c.store.applyEvent(c, rawEvent(EventMessageReceived, fmt.Sprintf("seed-%d", seq), m))
	got := findBySeq(t, c, convID, seq)
	if got == nil {
		t.Fatalf("seed 消息 seq=%d 未入库", seq)
	}
	return got
}

func findBySeq(t *testing.T, c *Client, convID string, seq int64) *Message {
	t.Helper()
	for _, m := range c.store.Messages(convID) {
		if m.Seq == seq {
			return m
		}
	}
	return nil
}

// ---- 会话列表 ----

func TestConversationsOrdering(t *testing.T) {
	c, _ := newTestClient(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	put := func(id string, at time.Time, ver int64) {
		c.store.PutConversation(&ConversationState{
			ConversationID: id, LastMessageAt: at.Format(time.RFC3339), Version: ver,
		})
	}
	put("c2", base.Add(time.Hour), 1)
	put("c1", base.Add(2*time.Hour), 1)
	put("c3", base.Add(time.Hour), 1) // 与 c2 同时间：按 ID 升序兜底

	got := c.store.Conversations()
	if len(got) != 3 {
		t.Fatalf("会话数 = %d, want 3", len(got))
	}
	want := []string{"c1", "c2", "c3"}
	for i, id := range want {
		if got[i].ConversationID != id {
			t.Errorf("第 %d 个会话 = %s, want %s", i, got[i].ConversationID, id)
		}
	}
}

// ---- 消息缓存 ----

func TestPutMessageOverwriteAndDelete(t *testing.T) {
	c, _ := newTestClient(t)
	m1 := &Message{ConversationID: "conv", Seq: 5, Type: "text", Body: json.RawMessage(`{"text":"v1"}`)}
	m2 := &Message{ConversationID: "conv", Seq: 5, Type: "text", Body: json.RawMessage(`{"text":"v2"}`)}
	c.store.PutMessage(m1)
	c.store.PutMessage(m2) // 同 seq 覆盖
	msgs := c.store.Messages("conv")
	if len(msgs) != 1 || string(msgs[0].Body) != `{"text":"v2"}` {
		t.Fatalf("同 seq 消息应被覆盖, got %d 条 body=%s", len(msgs), msgs[0].Body)
	}
	c.store.PutMessage(&Message{ConversationID: "conv", Seq: 3})
	c.store.DeleteMessages("conv", []int64{3, 99}) // 99 不存在，不应报错
	if msgs = c.store.Messages("conv"); len(msgs) != 1 || msgs[0].Seq != 5 {
		t.Fatalf("删除后应剩 seq=5, got %d 条", len(msgs))
	}
	c.store.DeleteMessages("conv", []int64{5})
	if msgs = c.store.Messages("conv"); len(msgs) != 0 {
		t.Fatalf("全部删除后应为空, got %d 条", len(msgs))
	}
}

func TestEventDeduplication(t *testing.T) {
	c, _ := newTestClient(t)
	e := rawEvent(EventMessageReceived, "evt-1", map[string]any{
		"conversation_id": "conv", "conversation_type": "single", "seq": 1,
		"sender": "alice", "recipient": "bob", "type": "text",
		"body": map[string]string{"text": "hi"}, "created_at": "2026-01-01T00:00:00Z",
	})
	c.store.applyEvent(c, e)
	c.store.applyEvent(c, e) // 同 ID 重复事件应被忽略
	if msgs := c.store.Messages("conv"); len(msgs) != 1 {
		t.Fatalf("重复事件应去重, got %d 条消息", len(msgs))
	}
}

// ---- message.received 三态 ----

func TestMessageReceivedNewConversation(t *testing.T) {
	c, _ := newTestClient(t)
	c.store.setSelf("bob")
	// 对端发来：建会话、peer=alice、未读+1
	c.store.applyEvent(c, rawEvent(EventMessageReceived, "e1", map[string]any{
		"conversation_id": "single:alice:bob", "conversation_type": "single", "seq": 1,
		"sender": "alice", "recipient": "bob", "type": "text",
		"body": map[string]string{"text": "hi"}, "created_at": "2026-01-01T00:00:00Z",
	}))
	st := c.store.Conversation("single:alice:bob")
	if st == nil || st.Peer != "alice" || st.MaxSeq != 1 || st.UnreadCount != 1 {
		t.Fatalf("新会话状态错误: %+v", st)
	}
	// 自己发出：peer=recipient、未读不变
	c.store.applyEvent(c, rawEvent(EventMessageReceived, "e2", map[string]any{
		"conversation_id": "single:alice:bob", "conversation_type": "single", "seq": 2,
		"sender": "bob", "recipient": "alice", "type": "text",
		"body": map[string]string{"text": "yo"}, "created_at": "2026-01-01T00:00:01Z",
	}))
	st = c.store.Conversation("single:alice:bob")
	if st.Peer != "alice" || st.MaxSeq != 2 || st.UnreadCount != 1 {
		t.Fatalf("自己发消息不应增加未读: %+v", st)
	}
	if len(c.store.Messages("single:alice:bob")) != 2 {
		t.Fatalf("应有 2 条消息")
	}
}

func TestMessageReceivedAppend(t *testing.T) {
	c, _ := newTestClient(t)
	c.store.setSelf("bob")
	seedMessage(t, c, "conv", 1, "a")
	// seq == max+1 直接追加
	c.store.applyEvent(c, rawEvent(EventMessageReceived, "e2", map[string]any{
		"conversation_id": "conv", "conversation_type": "single", "seq": 2,
		"sender": "alice", "recipient": "bob", "type": "text",
		"body": map[string]string{"text": "b"}, "created_at": "2026-01-01T00:00:02Z",
	}))
	if msgs := c.store.Messages("conv"); len(msgs) != 2 || msgs[1].Seq != 2 {
		t.Fatalf("seq=max+1 应追加, got %d 条", len(msgs))
	}
}

func TestMessageReceivedGapFetchesMissing(t *testing.T) {
	c, stub := newTestClient(t)
	c.store.setSelf("bob")
	seedMessage(t, c, "conv", 1, "a")
	// seq > max+1：收下这条并异步补缺口（stub 返回空 items）
	c.store.applyEvent(c, rawEvent(EventMessageReceived, "e5", map[string]any{
		"conversation_id": "conv", "conversation_type": "single", "seq": 5,
		"sender": "alice", "recipient": "bob", "type": "text",
		"body": map[string]string{"text": "e"}, "created_at": "2026-01-01T00:00:05Z",
	}))
	if m := findBySeq(t, c, "conv", 5); m == nil {
		t.Fatal("缺口消息应先收下")
	}
	if st := c.store.Conversation("conv"); st == nil || st.MaxSeq != 5 {
		t.Fatalf("MaxSeq 应推进到 5: %+v", st)
	}
	stub.waitFor(t, "/client/v1/conversations/conv/messages", 1) // 补缺请求已发出
}

func TestMessageReceivedStaleIgnored(t *testing.T) {
	c, _ := newTestClient(t)
	c.store.setSelf("bob")
	seedMessage(t, c, "conv", 5, "latest")
	// seq <= max：过期事件忽略
	c.store.applyEvent(c, rawEvent(EventMessageReceived, "e3", map[string]any{
		"conversation_id": "conv", "conversation_type": "single", "seq": 3,
		"sender": "alice", "recipient": "bob", "type": "text",
		"body": map[string]string{"text": "old"}, "created_at": "2026-01-01T00:00:03Z",
	}))
	if m := findBySeq(t, c, "conv", 3); m != nil {
		t.Fatal("过期消息不应入库")
	}
	if msgs := c.store.Messages("conv"); len(msgs) != 1 {
		t.Fatalf("应只有 1 条消息, got %d", len(msgs))
	}
}

// ---- message.changed 平铺补丁 ----

func TestMessageChangedRecalled(t *testing.T) {
	c, _ := newTestClient(t)
	c.store.setSelf("bob")
	seedMessage(t, c, "conv", 1, "将被撤回")
	c.store.applyEvent(c, rawEvent(EventMessageChanged, "c1", map[string]any{
		"conversation_id": "conv", "seq": 1, "message_version": 2,
		"change":   map[string]string{"kind": "recalled", "actor": "alice"},
		"recalled": map[string]string{"by": "alice", "at": "2026-01-01T00:01:00Z"},
	}))
	m := findBySeq(t, c, "conv", 1)
	if m == nil || m.Recalled == nil || m.Recalled.By != "alice" {
		t.Fatalf("撤回标记未生效: %+v", m)
	}
	if m.Body != nil || m.Ext != nil || m.Pinned != nil || m.Reactions != nil {
		t.Fatalf("撤回后应清空 body/ext/pinned/reactions: body=%s", m.Body)
	}
	if m.MessageVersion != 2 {
		t.Fatalf("MessageVersion 应为 2, got %d", m.MessageVersion)
	}
}

func TestMessageChangedEditedAndExt(t *testing.T) {
	c, _ := newTestClient(t)
	c.store.setSelf("bob")
	seedMessage(t, c, "conv", 1, "原始内容")
	c.store.applyEvent(c, rawEvent(EventMessageChanged, "c1", map[string]any{
		"conversation_id": "conv", "seq": 1, "message_version": 3,
		"change": map[string]string{"kind": "edited", "actor": "alice"},
		"body":   map[string]string{"text": "编辑后"},
		"ext":    map[string]any{"k": "v", "n": float64(2)},
		"edited": map[string]any{"at": "2026-01-01T00:02:00Z", "count": 1},
	}))
	m := findBySeq(t, c, "conv", 1)
	var b struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(m.Body, &b); err != nil || b.Text != "编辑后" {
		t.Fatalf("body 应更新为编辑后, got %s", m.Body)
	}
	if m.Ext["k"] != "v" || m.Ext["n"] != float64(2) {
		t.Fatalf("ext 应合并: %+v", m.Ext)
	}
	if m.Edited == nil || m.Edited.Count != 1 {
		t.Fatalf("edited 标记未生效: %+v", m.Edited)
	}
	// 旧版本事件忽略
	c.store.applyEvent(c, rawEvent(EventMessageChanged, "c2", map[string]any{
		"conversation_id": "conv", "seq": 1, "message_version": 2,
		"change": map[string]string{"kind": "edited", "actor": "alice"},
		"body":   map[string]string{"text": "旧版本"},
	}))
	m = findBySeq(t, c, "conv", 1)
	if string(m.Body) == `{"text":"旧版本"}` {
		t.Fatal("旧 MessageVersion 事件不应生效")
	}
}

func TestMessageChangedPinnedReactionsErased(t *testing.T) {
	c, _ := newTestClient(t)
	c.store.setSelf("bob")
	seedMessage(t, c, "conv", 1, "hello")
	// 置顶
	c.store.applyEvent(c, rawEvent(EventMessageChanged, "p1", map[string]any{
		"conversation_id": "conv", "seq": 1, "message_version": 2,
		"change": map[string]string{"kind": "pinned", "actor": "alice"},
		"pinned": map[string]string{"by": "alice", "at": "2026-01-01T00:03:00Z"},
	}))
	m := findBySeq(t, c, "conv", 1)
	if m.Pinned == nil || m.Pinned.By != "alice" {
		t.Fatalf("置顶未生效: %+v", m.Pinned)
	}
	// 回应
	c.store.applyEvent(c, rawEvent(EventMessageChanged, "r1", map[string]any{
		"conversation_id": "conv", "seq": 1, "message_version": 3,
		"change":    map[string]string{"kind": "reaction_added", "actor": "bob", "key": "👍"},
		"reactions": []map[string]any{{"key": "👍", "count": 1, "users": []string{"bob"}, "reacted": true}},
	}))
	if m = findBySeq(t, c, "conv", 1); m.Pinned == nil || len(m.Reactions) != 1 || m.Reactions[0].Key != "👍" {
		t.Fatalf("回应未生效或置顶被误清: pinned=%+v reactions=%+v", m.Pinned, m.Reactions)
	}
	// 取消置顶
	c.store.applyEvent(c, rawEvent(EventMessageChanged, "p2", map[string]any{
		"conversation_id": "conv", "seq": 1, "message_version": 4,
		"change": map[string]string{"kind": "unpinned", "actor": "alice"},
	}))
	if m = findBySeq(t, c, "conv", 1); m.Pinned != nil {
		t.Fatalf("取消置顶未生效: %+v", m.Pinned)
	}
	// 擦除
	c.store.applyEvent(c, rawEvent(EventMessageChanged, "e1", map[string]any{
		"conversation_id": "conv", "seq": 1, "message_version": 5,
		"change": map[string]string{"kind": "erased", "actor": "alice"},
	}))
	if m = findBySeq(t, c, "conv", 1); !m.Erased || m.Body != nil {
		t.Fatalf("擦除未生效: erased=%v body=%s", m.Erased, m.Body)
	}
}

func TestMessageChangedUnknownMessageIgnored(t *testing.T) {
	c, _ := newTestClient(t)
	// 本地没有该消息/会话：补丁应安全忽略
	c.store.applyEvent(c, rawEvent(EventMessageChanged, "x1", map[string]any{
		"conversation_id": "ghost", "seq": 9, "message_version": 2,
		"change": map[string]string{"kind": "recalled", "actor": "alice"},
	}))
	if msgs := c.store.Messages("ghost"); len(msgs) != 0 {
		t.Fatal("未知消息的补丁不应产生缓存")
	}
}

// ---- message.deleted / conversation_changed ----

func TestMessageDeleted(t *testing.T) {
	c, _ := newTestClient(t)
	c.store.setSelf("bob")
	seedMessage(t, c, "conv", 1, "a")
	seedMessage(t, c, "conv", 2, "b")
	c.store.applyEvent(c, rawEvent(EventMessageDeleted, "d1", map[string]any{
		"conversation_id": "conv", "seqs": []int64{1, 7},
	}))
	if m := findBySeq(t, c, "conv", 1); m != nil {
		t.Fatal("seq=1 应被删除")
	}
	if m := findBySeq(t, c, "conv", 2); m == nil {
		t.Fatal("seq=2 应保留")
	}
}

func TestConversationChangedClearedSeq(t *testing.T) {
	c, _ := newTestClient(t)
	c.store.setSelf("bob")
	seedMessage(t, c, "conv", 1, "a")
	seedMessage(t, c, "conv", 2, "b")
	seedMessage(t, c, "conv", 3, "c")
	c.store.PutConversation(&ConversationState{ConversationID: "conv", Version: 5, MaxSeq: 3})
	// 版本更高的会话状态 + cleared_seq 前进：删除本地不超过它的消息
	c.store.applyEvent(c, rawEvent(EventConversationChanged, "cv1", map[string]any{
		"conversation_id": "conv", "version": 6, "max_seq": 3, "cleared_seq": 2,
	}))
	if m := findBySeq(t, c, "conv", 1); m != nil || findBySeq(t, c, "conv", 2) != nil {
		t.Fatal("seq<=cleared_seq 的消息应被清理")
	}
	if m := findBySeq(t, c, "conv", 3); m == nil {
		t.Fatal("seq>cleared_seq 的消息应保留")
	}
	// 旧版本状态被忽略
	c.store.applyEvent(c, rawEvent(EventConversationChanged, "cv2", map[string]any{
		"conversation_id": "conv", "version": 2, "max_seq": 1,
	}))
	if st := c.store.Conversation("conv"); st == nil || st.Version != 6 {
		t.Fatalf("旧版本会话状态不应覆盖: %+v", st)
	}
}

func TestConversationChangedResyncTriggersSync(t *testing.T) {
	c, stub := newTestClient(t)
	c.store.applyEvent(c, rawEvent(EventConversationChanged, "rs1", map[string]any{"resync": true}))
	stub.waitFor(t, "/client/v1/conversations/recent", 1) // lightSync 完成的第一步
}

// ---- peer_read / typing ----

func TestPeerReadForwardOnly(t *testing.T) {
	c, _ := newTestClient(t)
	c.store.PutConversation(&ConversationState{ConversationID: "conv", PeerReadSeq: 10})
	c.store.applyEvent(c, rawEvent(EventPeerRead, "r1", map[string]any{
		"conversation_id": "conv", "peer": "alice", "read_seq": 15,
	}))
	if st := c.store.Conversation("conv"); st.PeerReadSeq != 15 {
		t.Fatalf("peer_read_seq 应推进到 15, got %d", st.PeerReadSeq)
	}
	c.store.applyEvent(c, rawEvent(EventPeerRead, "r2", map[string]any{
		"conversation_id": "conv", "peer": "alice", "read_seq": 12,
	}))
	if st := c.store.Conversation("conv"); st.PeerReadSeq != 15 {
		t.Fatalf("回退的 peer_read_seq 不应生效, got %d", st.PeerReadSeq)
	}
}

func TestTypingExpiry(t *testing.T) {
	c, _ := newTestClient(t)
	c.store.applyEvent(c, rawEvent(EventTyping, "t1", map[string]any{
		"conversation_id": "conv", "username": "alice",
	}))
	if tp := c.store.Typing("conv"); tp == nil || tp.Username != "alice" {
		t.Fatalf("typing 应有效: %+v", tp)
	}
	// 手动把时间拨回 11 秒前 → 过期
	c.store.mu.Lock()
	tinfo := c.store.typing["conv"]
	tinfo.At = time.Now().Add(-11 * time.Second)
	c.store.typing["conv"] = tinfo
	c.store.mu.Unlock()
	if tp := c.store.Typing("conv"); tp != nil {
		t.Fatal("超过 10 秒的 typing 应过期")
	}
}

// ---- 上传 kind 推断 ----

func TestUploadKind(t *testing.T) {
	cases := map[string]string{
		"image/png": "image", "image/jpeg": "image",
		"audio/mpeg": "voice", "audio/wav": "voice",
		"video/mp4": "video", "application/pdf": "file",
		"": "file", "text/plain": "file",
	}
	for ct, want := range cases {
		if got := uploadKind(ct); got != want {
			t.Errorf("uploadKind(%q) = %q, want %q", ct, got, want)
		}
	}
}

// ---- SyncAll 全量同步（对 stub 服务） ----

func TestSyncAllAgainstStub(t *testing.T) {
	stub := newSyncStub(t)
	c, err := New(Config{AppKey: "org#testapp", APIURL: stub.srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	c.store.setSelf("bob")
	// 注入一条会话让 BackfillLastMessages 有对象可补
	c.store.PutConversation(&ConversationState{ConversationID: "conv", MaxSeq: 3})
	n, err := c.SyncAll(context.Background())
	if err != nil {
		t.Fatalf("SyncAll: %v", err)
	}
	// stub 全部返回空：total=0；注入的会话仅作为 BackfillLastMessages 的对象保留
	if n != 0 {
		t.Fatalf("SyncAll total = %d, want 0（stub 返回空）", n)
	}
	if got := c.Store().Conversation("conv"); got == nil {
		t.Fatal("注入的会话不应被清掉")
	}
	for _, p := range []string{
		"/client/v1/conversations/recent",
		"/client/v1/conversations/group-digest",
		"/client/v1/conversations/sync",
		"/client/v1/conversations/last-messages",
	} {
		if stub.hitCount(p) == 0 {
			t.Errorf("同步流程未请求 %s", p)
		}
	}
}
