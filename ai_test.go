package im

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestAIClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c, err := New(Config{AppKey: "1#demo", APIURL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	c.setSession(&Session{AccessToken: "t"})
	return c
}

func event(name string, data any) *Event {
	raw, _ := json.Marshal(data)
	return &Event{Name: name, Data: raw}
}

func texts(s *AIStreamState) string {
	var b strings.Builder
	for _, p := range s.Parts {
		b.WriteString(p.Type + ":" + p.Text + p.State + "|")
	}
	return b.String()
}

func TestAIDeltasApplyInOrderAndIgnoreDuplicates(t *testing.T) {
	c := newTestAIClient(t, func(w http.ResponseWriter, r *http.Request) { t.Errorf("不应请求 %s", r.URL.Path) })
	var updates []AIStreamState
	c.AI().OnAI(func(s AIStreamState) { updates = append(updates, s) })
	c.AI().handle(event(EventMessageReceived, map[string]any{"conversation_id": "c1", "seq": 9, "type": "ai",
		"body": map[string]any{"stream_id": "s1", "status": "streaming", "parts": []any{}}}))
	c.AI().handle(event(EventAIDelta, map[string]any{"stream_id": "s1", "conversation_id": "c1", "message_seq": 9, "from_seq": 1, "seq": 2,
		"ops": []any{
			map[string]any{"op": "append", "index": 0, "type": "reasoning", "text": "想"},
			map[string]any{"op": "put", "index": 1, "part": map[string]any{"type": "tool", "id": "t1", "name": "search", "state": "running"}},
		}}))
	c.AI().handle(event(EventAIDelta, map[string]any{"stream_id": "s1", "from_seq": 3, "seq": 3,
		"ops": []any{map[string]any{"op": "append", "index": 2, "type": "text", "text": "你"}}}))
	c.AI().handle(event(EventAIDelta, map[string]any{"stream_id": "s1", "from_seq": 3, "seq": 3, // 重复的整段
		"ops": []any{map[string]any{"op": "append", "index": 2, "type": "text", "text": "你"}}}))
	c.AI().handle(event(EventAIDelta, map[string]any{"stream_id": "s1", "from_seq": 4, "seq": 4,
		"ops": []any{
			map[string]any{"op": "append", "index": 2, "type": "text", "text": "好"},
			map[string]any{"op": "put", "index": 1, "part": map[string]any{"type": "tool", "id": "t1", "name": "search", "state": "done"}},
		}}))
	state := c.AI().State("s1")
	if state == nil || state.Seq != 4 || texts(state) != "reasoning:想|tool:done|text:你好|" {
		t.Fatalf("状态 = %+v", state)
	}
	if len(updates) != 4 || updates[0].Seq != 0 {
		t.Fatalf("回调 %d 次：%+v", len(updates), updates)
	}
	// message.changed 的 ai_finished 带最终正文：结束跟踪，回调结束状态
	c.AI().handle(event(EventMessageChanged, map[string]any{"conversation_id": "c1", "seq": 9, "message_version": 2,
		"change": map[string]any{"kind": "ai_finished"}, "body": map[string]any{"stream_id": "s1", "status": "completed"}}))
	if c.AI().State("s1") != nil || updates[len(updates)-1].Status != AICompleted {
		t.Fatalf("结束后应为 nil：%+v", updates[len(updates)-1])
	}
}

func TestAIGapResumesFromServer(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	c := newTestAIClient(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path+"?"+r.URL.RawQuery)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "streaming", "deltas": []any{
			map[string]any{"seq": 2, "ops": []any{map[string]any{"op": "append", "index": 0, "type": "text", "text": "好"}}},
			map[string]any{"seq": 3, "ops": []any{map[string]any{"op": "append", "index": 0, "type": "text", "text": "！"}}},
		}})
	})
	c.AI().handle(event(EventAIDelta, map[string]any{"stream_id": "s2", "from_seq": 1, "seq": 1,
		"ops": []any{map[string]any{"op": "append", "index": 0, "type": "text", "text": "你"}}}))
	c.AI().handle(event(EventAIDelta, map[string]any{"stream_id": "s2", "from_seq": 3, "seq": 3, // 跳号：缺 2
		"ops": []any{map[string]any{"op": "append", "index": 0, "type": "text", "text": "！"}}}))
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s := c.AI().State("s2"); s != nil && s.Seq == 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	s := c.AI().State("s2")
	if s == nil || s.Seq != 3 || texts(s) != "text:你好！|" {
		t.Fatalf("续传后的状态 = %+v", s)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 1 || paths[0] != "/client/v1/ai/streams/s2/deltas?after_seq=1" {
		t.Fatalf("续传请求 = %v", paths)
	}
}

func TestAISnapshotStopAndBodyOf(t *testing.T) {
	c := newTestAIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/client/v1/ai/streams/s3/stop" {
			t.Errorf("请求 %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"stream": map[string]any{"stream_id": "s3", "status": "stopped", "seq": 5,
			"parts": []any{map[string]any{"type": "text", "text": "半句"}}}})
	})
	c.AI().handle(event(EventAISnapshot, map[string]any{"stream_id": "s3", "conversation_id": "c1", "message_seq": 4, "status": "streaming", "seq": 5,
		"parts": []any{map[string]any{"type": "text", "text": "半句"}}}))
	if s := c.AI().State("s3"); s == nil || s.Seq != 5 || texts(s) != "text:半句|" {
		t.Fatalf("快照 = %+v", s)
	}
	view, err := c.AI().Stop(c.bgCtx(), "s3")
	if err != nil || view.Status != AIStopped || c.AI().State("s3") != nil {
		t.Fatalf("停止 = %+v, %v", view, err)
	}
	body := BodyOf(&Message{Type: "ai", Body: json.RawMessage(`{"stream_id":"s3","status":"stopped","parts":[{"type":"text","text":"半句"}],"text":"半句"}`)})
	if body == nil || body.Status != AIStopped || body.Text != "半句" {
		t.Fatalf("BodyOf = %+v", body)
	}
	if BodyOf(&Message{Type: "text", Body: json.RawMessage(`{"text":"x"}`)}) != nil {
		t.Fatal("不是 ai 消息应为 nil")
	}
	// 类型不符的增量不应用
	state := &AIStreamState{Parts: []AIPart{{Type: "text", Text: "a"}}}
	if applyAIOps(state, []aiOp{{Op: "append", Index: 0, Type: "reasoning", Text: "b"}}) || state.Parts[0].Text != "a" {
		t.Fatal("类型不符应拒绝")
	}
}
