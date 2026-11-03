package im

// AI 回复（《AI对话/3.接口设计》第 5、7.2 节）：长连接上的 ai.delta、ai.snapshot 按 stream_id 维护生成中的累积内容。
// from_seq 接上本地序号时应用，已有的整段忽略，跳号时续传；收到消息的 ai_finished 变更后重新读取这条消息，
// 以消息正文为准并丢弃本地状态。界面用 AI().State(streamID) 读取“生成中”的内容，OnAI 订阅变化。

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"sync"
)

// AI 回复相关的事件名。
const (
	EventAIDelta    = "ai.delta"
	EventAISnapshot = "ai.snapshot"
)

// AI 流的状态。
const (
	AIStreaming = "streaming"
	AICompleted = "completed"
	AIStopped   = "stopped"
	AIFailed    = "failed"
)

// maxAIStreams 是同时跟踪的流的上限。
const maxAIStreams = 200

// AIPart 是 AI 回复的一个片段：text、reasoning 为文字；image、file 为附件；tool 为工具调用的进度。
type AIPart struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	URL      string `json:"url,omitempty"`
	Width    int64  `json:"width,omitempty"`
	Height   int64  `json:"height,omitempty"`
	Alt      string `json:"alt,omitempty"`
	Name     string `json:"name,omitempty"`
	Size     int64  `json:"size,omitempty"`
	MimeType string `json:"mime_type,omitempty"`
	ID       string `json:"id,omitempty"`
	Title    string `json:"title,omitempty"`
	State    string `json:"state,omitempty"`
}

// AIError 是生成中断的原因。
type AIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// AIStreamState 是生成中的回复在本地的状态。
type AIStreamState struct {
	StreamID       string   `json:"stream_id"`
	ConversationID string   `json:"conversation_id"`
	MessageSeq     int64    `json:"message_seq"`
	Status         string   `json:"status"`
	Seq            int64    `json:"seq"` // 已应用的最后一段增量
	Parts          []AIPart `json:"parts"`
}

// AIStreamView 是停止生成、读取流的结果。
type AIStreamView struct {
	StreamID       string   `json:"stream_id"`
	Bot            string   `json:"bot"`
	ConversationID string   `json:"conversation_id"`
	MessageSeq     int64    `json:"message_seq"`
	Status         string   `json:"status"`
	Seq            int64    `json:"seq"`
	FinishReason   *string  `json:"finish_reason"`
	Error          *AIError `json:"error"`
	Parts          []AIPart `json:"parts"`
}

// AIBody 是 type 为 ai 的消息正文。
type AIBody struct {
	StreamID     string   `json:"stream_id"`
	Status       string   `json:"status"`
	Parts        []AIPart `json:"parts"`
	Text         string   `json:"text"`
	FinishReason *string  `json:"finish_reason"`
	Error        *AIError `json:"error"`
	LastSeq      int64    `json:"last_seq"`
}

// aiOp 是增量中的一个操作。
type aiOp struct {
	Op    string  `json:"op"`
	Index int     `json:"index"`
	Type  string  `json:"type,omitempty"`
	Text  string  `json:"text,omitempty"`
	Part  *AIPart `json:"part,omitempty"`
}

type aiDelta struct {
	StreamID       string `json:"stream_id"`
	ConversationID string `json:"conversation_id"`
	MessageSeq     int64  `json:"message_seq"`
	FromSeq        int64  `json:"from_seq"`
	Seq            int64  `json:"seq"`
	Ops            []aiOp `json:"ops"`
}

type aiSnapshot struct {
	StreamID       string   `json:"stream_id"`
	ConversationID string   `json:"conversation_id"`
	MessageSeq     int64    `json:"message_seq"`
	Status         string   `json:"status"`
	Seq            int64    `json:"seq"`
	Parts          []AIPart `json:"parts"`
}

// AIModule 是 AI 回复的本地状态与接口，用 Client.AI() 取得。
type AIModule struct {
	c        *Client
	mu       sync.Mutex
	streams  map[string]*AIStreamState
	order    []string // 加入顺序，超过上限时丢弃最早的
	resuming map[string]bool
	hmu      sync.RWMutex
	handlers []func(AIStreamState)
}

func newAIModule(c *Client) *AIModule {
	return &AIModule{c: c, streams: map[string]*AIStreamState{}, resuming: map[string]bool{}}
}

// AI 返回 AI 回复的模块。
func (c *Client) AI() *AIModule { return c.ai }

// OnAI 订阅生成中的回复的变化：开始、每次更新和结束时回调（结束时 Status 不再是 streaming，Parts 为空，改读消息正文）。
func (a *AIModule) OnAI(fn func(AIStreamState)) {
	a.hmu.Lock()
	a.handlers = append(a.handlers, fn)
	a.hmu.Unlock()
}

// State 返回本地跟踪的生成中的回复（副本）；结束后或从未收到时为 nil。
func (a *AIModule) State(streamID string) *AIStreamState {
	a.mu.Lock()
	defer a.mu.Unlock()
	if s := a.streams[streamID]; s != nil {
		copied := *s
		copied.Parts = append([]AIPart(nil), s.Parts...)
		return &copied
	}
	return nil
}

// Stop 停止生成：只有接收者可以停止；已结束的返回当前状态。
func (a *AIModule) Stop(ctx context.Context, streamID string) (*AIStreamView, error) {
	var resp struct {
		Stream AIStreamView `json:"stream"`
	}
	if err := a.c.request(ctx, http.MethodPost, "/client/v1/ai/streams/"+url.PathEscape(streamID)+"/stop", nil, &resp); err != nil {
		return nil, err
	}
	a.end(resp.Stream.StreamID, resp.Stream.Status)
	return &resp.Stream, nil
}

// Get 从服务端读取流和当前累积的内容；结束超过 10 分钟的返回 not_found，改读消息。
func (a *AIModule) Get(ctx context.Context, streamID string) (*AIStreamView, error) {
	var resp struct {
		Stream AIStreamView `json:"stream"`
	}
	if err := a.c.getJSON(ctx, "/client/v1/ai/streams/"+url.PathEscape(streamID), nil, &resp); err != nil {
		return nil, err
	}
	return &resp.Stream, nil
}

// BodyOf 解析消息正文中的 AI 回复；不是 ai 类型时为 nil。
func BodyOf(m *Message) *AIBody {
	if m == nil || m.Type != "ai" || len(m.Body) == 0 {
		return nil
	}
	var body AIBody
	if json.Unmarshal(m.Body, &body) != nil {
		return nil
	}
	return &body
}

// handle 处理与 AI 有关的事件（在 Store 应用之后调用，事件按到达顺序串行）。
func (a *AIModule) handle(e *Event) {
	switch e.Name {
	case EventAIDelta:
		var d aiDelta
		if json.Unmarshal(e.Data, &d) == nil && d.StreamID != "" {
			a.onDelta(d)
		}
	case EventAISnapshot:
		var s aiSnapshot
		if json.Unmarshal(e.Data, &s) == nil && s.StreamID != "" {
			a.applySnapshot(s)
		}
	case EventMessageReceived:
		var m Message
		if json.Unmarshal(e.Data, &m) == nil {
			a.onMessage(&m)
		}
	case EventMessageChanged:
		// ai_finished 带最终的正文（Store 已整体替换）：按其中的 stream_id 结束本地跟踪；没有正文时重新读取这条消息
		var d MessageChangedData
		if json.Unmarshal(e.Data, &d) == nil && d.Change.Kind == "ai_finished" {
			var body AIBody
			if len(d.Body) > 0 && json.Unmarshal(d.Body, &body) == nil && body.StreamID != "" {
				a.end(body.StreamID, body.Status)
			} else {
				go a.refetch(d.ConversationID, d.Seq)
			}
		}
	}
}

// onMessage：status 为 streaming 的 ai 消息开始跟踪；已结束的结束跟踪。
func (a *AIModule) onMessage(m *Message) {
	body := BodyOf(m)
	if body == nil {
		return
	}
	if body.Status != AIStreaming {
		a.end(body.StreamID, body.Status)
		return
	}
	a.mu.Lock()
	if a.streams[body.StreamID] != nil {
		a.mu.Unlock()
		return
	}
	state := &AIStreamState{StreamID: body.StreamID, ConversationID: m.ConversationID, MessageSeq: m.Seq, Status: AIStreaming}
	a.rememberLocked(state)
	copied := *state
	a.mu.Unlock()
	a.emit(copied)
}

func (a *AIModule) onDelta(d aiDelta) {
	a.mu.Lock()
	state := a.streams[d.StreamID]
	if state == nil {
		state = &AIStreamState{StreamID: d.StreamID, ConversationID: d.ConversationID, MessageSeq: d.MessageSeq, Status: AIStreaming}
		a.rememberLocked(state)
	}
	if d.Seq <= state.Seq { // 已有的整段
		a.mu.Unlock()
		return
	}
	if d.FromSeq != state.Seq+1 || !applyAIOps(state, d.Ops) {
		a.mu.Unlock()
		go a.resume(d.StreamID)
		return
	}
	state.Seq = d.Seq
	copied := *state
	copied.Parts = append([]AIPart(nil), state.Parts...)
	a.mu.Unlock()
	a.emit(copied)
}

func (a *AIModule) applySnapshot(s aiSnapshot) {
	a.mu.Lock()
	if current := a.streams[s.StreamID]; current != nil && current.Seq > s.Seq {
		a.mu.Unlock()
		return
	}
	state := &AIStreamState{StreamID: s.StreamID, ConversationID: s.ConversationID, MessageSeq: s.MessageSeq, Status: s.Status, Seq: s.Seq,
		Parts: append([]AIPart(nil), s.Parts...)}
	a.rememberLocked(state)
	copied := *state
	a.mu.Unlock()
	if s.Status != AIStreaming {
		a.end(s.StreamID, s.Status)
		return
	}
	a.emit(copied)
}

// resume 在跳号或增量无法应用时续传：取本地序号之后的增量，落后太多时整体替换为快照；流已结束时等消息的变更。
func (a *AIModule) resume(streamID string) {
	a.mu.Lock()
	state := a.streams[streamID]
	if state == nil || a.resuming[streamID] {
		a.mu.Unlock()
		return
	}
	a.resuming[streamID] = true
	after := state.Seq
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		delete(a.resuming, streamID)
		a.mu.Unlock()
	}()
	var result struct {
		Status   string      `json:"status"`
		Deltas   []aiDelta   `json:"deltas"`
		Snapshot *aiSnapshot `json:"snapshot"`
	}
	q := url.Values{"after_seq": {strconv.FormatInt(after, 10)}}
	if err := a.c.getJSON(a.c.bgCtx(), "/client/v1/ai/streams/"+url.PathEscape(streamID)+"/deltas", q, &result); err != nil {
		a.c.logf("debug", "ai", "续传失败", map[string]any{"stream_id": streamID, "error": err.Error()})
		return
	}
	if result.Snapshot != nil {
		a.applySnapshot(*result.Snapshot)
		return
	}
	if result.Status != AIStreaming {
		a.end(streamID, result.Status)
		return
	}
	a.mu.Lock()
	current := a.streams[streamID]
	if current == nil {
		a.mu.Unlock()
		return
	}
	for _, d := range result.Deltas {
		if d.Seq != current.Seq+1 {
			continue
		}
		if !applyAIOps(current, d.Ops) {
			break
		}
		current.Seq = d.Seq
	}
	copied := *current
	copied.Parts = append([]AIPart(nil), current.Parts...)
	a.mu.Unlock()
	a.emit(copied)
}

// refetch 在收到 ai_finished 后重新读取这条消息（正文以它为准），并结束本地跟踪。
func (a *AIModule) refetch(conversationID string, seq int64) {
	items, err := a.c.fetchMessages(a.c.bgCtx(), conversationID, seq-1, 0, 1)
	if err != nil {
		a.c.logf("debug", "ai", "读取已结束的 AI 消息失败", map[string]any{"conversation_id": conversationID, "seq": seq, "error": err.Error()})
		return
	}
	for _, m := range items {
		if body := BodyOf(m); body != nil && m.Seq == seq {
			a.end(body.StreamID, body.Status)
		}
	}
}

// end 结束跟踪，通知订阅者（Status 为结束时的状态，Parts 为空）。
func (a *AIModule) end(streamID, status string) {
	a.mu.Lock()
	state := a.streams[streamID]
	if state == nil {
		a.mu.Unlock()
		return
	}
	delete(a.streams, streamID)
	ended := AIStreamState{StreamID: streamID, ConversationID: state.ConversationID, MessageSeq: state.MessageSeq, Status: status, Seq: state.Seq}
	a.mu.Unlock()
	a.emit(ended)
}

func (a *AIModule) rememberLocked(state *AIStreamState) {
	if _, ok := a.streams[state.StreamID]; !ok {
		a.order = append(a.order, state.StreamID)
	}
	a.streams[state.StreamID] = state
	for len(a.streams) > maxAIStreams && len(a.order) > 0 {
		oldest := a.order[0]
		a.order = a.order[1:]
		delete(a.streams, oldest)
	}
	if len(a.order) > 4*maxAIStreams { // 清理已结束的流留下的 ID
		kept := a.order[:0]
		for _, id := range a.order {
			if a.streams[id] != nil {
				kept = append(kept, id)
			}
		}
		a.order = kept
	}
}

func (a *AIModule) emit(state AIStreamState) {
	a.hmu.RLock()
	handlers := make([]func(AIStreamState), len(a.handlers))
	copy(handlers, a.handlers)
	a.hmu.RUnlock()
	for _, h := range handlers {
		func() {
			defer func() { _ = recover() }()
			h(state)
		}()
	}
}

// applyAIOps 按服务端的规则应用操作：append 向文字片段追加（index 等于片段数时新建），put 整体设置；
// 索引跳号或类型不符时返回 false，不修改状态。
func applyAIOps(state *AIStreamState, ops []aiOp) bool {
	parts := append([]AIPart(nil), state.Parts...)
	for _, op := range ops {
		if op.Index < 0 || op.Index > len(parts) {
			return false
		}
		switch op.Op {
		case "append":
			if op.Index == len(parts) {
				parts = append(parts, AIPart{Type: op.Type, Text: op.Text})
			} else if parts[op.Index].Type != op.Type {
				return false
			} else {
				parts[op.Index].Text += op.Text
			}
		case "put":
			if op.Part == nil {
				return false
			}
			if op.Index == len(parts) {
				parts = append(parts, *op.Part)
			} else {
				parts[op.Index] = *op.Part
			}
		default:
			return false
		}
	}
	state.Parts = parts
	return true
}
