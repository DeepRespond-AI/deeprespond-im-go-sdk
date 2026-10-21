package im

// WS 长连接生命周期（《网关 3.接口设计》第 3 节）：连接 → 10 秒内 auth → 心跳 →
// 请求-回复关联 → 事件分发 → 按关闭码决定的重连策略（1s 起指数退避至 60s ±50% 抖动）。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// ConnectionState 连接状态。
type ConnectionState string

const (
	StateDisconnected ConnectionState = "disconnected"
	StateConnecting   ConnectionState = "connecting"
	StateConnected    ConnectionState = "connected" // auth 完成
)

// 关闭码（《网关 3.接口设计》第 3.9 节）。
const (
	closeUnauthenticated = 4001
	closeSessionRevoked  = 4003
	closeReplaced        = 4008
	closeReconnectHint   = 4010
	closeUpgradeRequired = 4014
	closeIdleTimeout     = 4012
)

// 不应重连的 4001 reason。
var noRetryReasons = map[string]bool{
	"device_mismatch": true, "app_deleted": true, "tenant_closed": true,
}

// EventHandler 事件回调。事件在独立 goroutine 顺序分发。
type EventHandler func(e Event)

// wsReply 一条在途请求的回复。
type wsReply struct {
	data json.RawMessage
	err  *APIError
}

type wsRequest struct {
	ch chan wsReply
}

// wsFrame 服务端下行帧。
type wsFrame struct {
	Type       string          `json:"type"` // reply|pong|event|disconnect
	ID         string          `json:"id,omitempty"`
	Data       json.RawMessage `json:"data,omitempty"`
	Error      *APIError       `json:"error,omitempty"`
	Event      string          `json:"event,omitempty"`
	At         string          `json:"at,omitempty"`
	Code       int             `json:"code,omitempty"`
	Message    string          `json:"message,omitempty"`
	RetryAfter *float64        `json:"retry_after_seconds,omitempty"`
}

// wsAuthReply auth 回复 data。
type wsAuthReply struct {
	ConnectionID  string `json:"connection_id"`
	SessionID     string `json:"session_id"`
	Username      string `json:"username"`
	HeartbeatSec  int    `json:"heartbeat_interval_seconds"`
	TokenExpires  string `json:"token_expires_at"`
	ConfigVersion int64  `json:"config_version"`
}

// wsManager 一条长连接的完整生命周期。由 Client 持有。
type wsManager struct {
	c *Client

	mu        sync.Mutex
	state     ConnectionState
	running   bool
	manual    bool
	conn      *frameConn // auth 完成后挂上
	pending   map[string]chan wsReply
	nextID    uint64
	gen       int // 连接代数：旧 loop 检测换代后退出
	stop      chan struct{}
	retryHint time.Duration // 服务端 retry_after_seconds 建议

	evMu     sync.RWMutex
	handlers []EventHandler

	stateMu sync.RWMutex
	stateFn func(ConnectionState)
}

func (w *wsManager) setState(s ConnectionState) {
	w.stateMu.RLock()
	fn := w.stateFn
	w.stateMu.RUnlock()
	w.mu.Lock()
	w.state = s
	w.mu.Unlock()
	if fn != nil {
		go fn(s)
	}
}

// State 返回当前连接状态。
func (c *Client) State() ConnectionState {
	c.mu.RLock()
	w := c.ws
	c.mu.RUnlock()
	if w == nil {
		return StateDisconnected
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.state
}

// OnState 注册连接状态变化回调。
func (c *Client) OnState(fn func(ConnectionState)) {
	c.ensureWS()
	c.ws.stateMu.Lock()
	c.ws.stateFn = fn
	c.ws.stateMu.Unlock()
}

// OnEvent 注册事件回调（可多个）。
func (c *Client) OnEvent(fn EventHandler) {
	c.ensureWS()
	c.ws.evMu.Lock()
	c.ws.handlers = append(c.ws.handlers, fn)
	c.ws.evMu.Unlock()
}

func (c *Client) ensureWS() {
	c.mu.Lock()
	if c.ws == nil {
		c.ws = &wsManager{c: c, pending: map[string]chan wsReply{}}
	}
	c.mu.Unlock()
}

// Connect 建立长连接并保持（内部自动重连）。必须已登录。立即返回，连接在后台进行。
func (c *Client) Connect() error {
	if c.token() == "" {
		return errors.New("im: 未登录，无法建立长连接")
	}
	c.ensureWS()
	w := c.ws
	w.mu.Lock()
	defer w.mu.Unlock()
	w.manual = false
	if w.running {
		return nil
	}
	w.running = true
	w.stop = make(chan struct{})
	go w.loop()
	return nil
}

// ConnectWait 建立长连接并等待首次 auth 完成（或失败）。
func (c *Client) ConnectWait(ctx context.Context) error {
	if err := c.Connect(); err != nil {
		return err
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if c.State() == StateConnected {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return fmt.Errorf("im: 连接超时，当前状态 %s", c.State())
}

// Disconnect 主动断开且不再自动重连。
func (c *Client) Disconnect() {
	c.mu.RLock()
	w := c.ws
	c.mu.RUnlock()
	if w == nil {
		return
	}
	w.mu.Lock()
	w.manual = true
	w.gen++
	if w.stop != nil {
		close(w.stop)
		w.stop = nil
	}
	w.running = false
	t := w.conn
	w.conn = nil
	w.mu.Unlock()
	w.setState(StateDisconnected)
	w.failPending(errors.New("im: 连接已主动断开"))
	if t != nil {
		t.CloseHandshake(1000, "client closing")
	}
}

// loop 连接主循环：连 → auth → 读帧直到断开 → 按需重连（指数退避）。
func (w *wsManager) loop() {
	backoff := time.Second
	for {
		w.mu.Lock()
		manual, stop := w.manual, w.stop
		w.mu.Unlock()
		if manual {
			return
		}
		w.setState(StateConnecting)

		delay := backoff
		if delay > time.Second { // 抖动 ±50%
			delay = time.Duration(float64(delay) * (0.5 + rand.Float64()*0.5))
		}
		if w.retryHint > delay {
			delay = w.retryHint
		}
		if delay > 60*time.Second {
			delay = 60 * time.Second
		}

		tok := w.c.token()
		if tok == "" {
			w.setState(StateDisconnected)
			w.c.logf("warn", "ws", "未登录，停止重连", nil)
			return
		}

		t, err := dialFrame(w.c.cfg.WSURL, 10*time.Second)
		if err != nil {
			w.c.logf("warn", "ws", "连接失败", map[string]any{"error": err.Error(), "retry_in": delay.String()})
			w.setState(StateDisconnected)
			if !sleepCtx(stop, delay) {
				return
			}
			backoff = minDur(backoff*2, 60*time.Second)
			continue
		}

		authData, _ := json.Marshal(map[string]any{
			"token":       tok,
			"device_id":   w.c.cfg.DeviceID,
			"sdk_version": w.c.cfg.SDKVersion,
			"os_version":  w.c.cfg.OSVersion,
			"app_version": w.c.cfg.AppVersion,
			"state":       "foreground",
		})
		authReply, aerr := authHandshake(t, authData, 10*time.Second)
		if aerr != nil {
			t.closeRaw()
			var closeErr *wsCloseError
			if errors.As(aerr, &closeErr) {
				if closeErr.Code == closeUnauthenticated && w.c.refreshToken() != "" {
					// 令牌过期：续期一次后立刻重试
					if rerr := w.c.Refresh(w.c.bgCtx()); rerr == nil {
						backoff = time.Second
						continue
					}
				}
				if closeErr.Code == closeSessionRevoked || closeErr.Code == closeUpgradeRequired ||
					(closeErr.Code == closeUnauthenticated && noRetryReasons[closeErr.Reason]) {
					w.c.logf("error", "ws", "认证被拒绝，停止重连", map[string]any{"reason": closeErr.Reason})
					w.setState(StateDisconnected)
					return
				}
			}
			w.setState(StateDisconnected)
			if !sleepCtx(stop, delay) {
				return
			}
			backoff = minDur(backoff*2, 60*time.Second)
			continue
		}
		var ar wsAuthReply
		_ = json.Unmarshal(authReply, &ar)
		w.c.logf("info", "ws", "已连接", map[string]any{
			"username": ar.Username, "heartbeat_seconds": ar.HeartbeatSec,
		})

		// 成功：重置退避，挂 transport，进入读循环
		backoff = time.Second
		w.mu.Lock()
		w.conn = t
		w.mu.Unlock()
		w.setState(StateConnected)

		hbErr := w.readLoop(t, &ar)

		w.mu.Lock()
		stillMine := !w.manual
		if w.conn == t {
			w.conn = nil
		}
		w.mu.Unlock()
		t.closeRaw()
		w.failPending(fmt.Errorf("im: 连接已断开"))
		w.setState(StateDisconnected)
		if !stillMine {
			return
		}
		w.c.logf("warn", "ws", "连接断开", map[string]any{"reason": fmt.Sprint(hbErr)})

		// 按关闭码决定是否重连
		var closeErr *wsCloseError
		if errors.As(hbErr, &closeErr) {
			switch closeErr.Code {
			case closeSessionRevoked, closeUpgradeRequired:
				w.c.logf("error", "ws", "会话已吊销，停止重连", nil)
				return
			case closeUnauthenticated:
				if noRetryReasons[closeErr.Reason] {
					return
				}
				// token 过期：立即尝试续期，成功则马上重连
				if w.c.refreshToken() != "" && w.c.Refresh(w.c.bgCtx()) == nil {
					backoff = time.Second
					continue
				}
				w.c.logf("error", "ws", "续期失败，停止重连", nil)
				return
			}
			if closeErr.RetryAfter > 0 {
				w.retryHint = time.Duration(closeErr.RetryAfter*float64(time.Second)) + 500*time.Millisecond
			}
		}
		if !sleepCtx(stop, delay) {
			return
		}
		backoff = minDur(backoff*2, 60*time.Second)
	}
}

// readLoop 读帧 + 心跳，直到连接结束。
func (w *wsManager) readLoop(t *frameConn, ar *wsAuthReply) error {
	heartbeat := time.Duration(ar.HeartbeatSec) * time.Second
	if heartbeat <= 0 {
		heartbeat = 30 * time.Second
	}
	readErr := make(chan error, 1)
	go func() {
		for {
			payload, err := t.ReadText(time.Now().Add(3*heartbeat + 15*time.Second))
			if err != nil {
				readErr <- err
				return
			}
			var f wsFrame
			if json.Unmarshal(payload, &f) != nil {
				continue // 忽略无法解析的帧
			}
			switch f.Type {
			case "reply":
				w.deliverReply(&f)
			case "event":
				w.dispatch(&f)
			case "pong":
				// 心跳由发送侧跟踪即可
			case "disconnect":
				readErr <- &wsCloseError{Code: f.Code, Reason: f.Message}
				return
			default:
				// 忽略未知 type
			}
		}
	}()

	lastSent := time.Now()
	ticker := time.NewTicker(heartbeat / 4)
	defer ticker.Stop()
	for {
		select {
		case err := <-readErr:
			return err
		case <-ticker.C:
			w.mu.Lock()
			connOK := w.conn == t
			w.mu.Unlock()
			if !connOK {
				return nil // 被接管/断开
			}
			if time.Since(lastSent) >= heartbeat {
				if err := w.sendRaw(t, "ping", nil, ""); err == nil {
					lastSent = time.Now()
				}
			}
		}
	}
}

// ---- 请求-回复 ----

// WSCall 通过 WS 发起一次 op 请求并等待回复。
func (c *Client) WSCall(ctx context.Context, op string, data any) (json.RawMessage, error) {
	c.ensureWS()
	w := c.ws
	var payload []byte
	if data != nil {
		b, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		payload = b
	}
	w.mu.Lock()
	if w.conn == nil {
		w.mu.Unlock()
		return nil, errors.New("im: 长连接未就绪")
	}
	t := w.conn
	w.nextID++
	id := fmt.Sprintf("%d", w.nextID)
	ch := make(chan wsReply, 1)
	w.pending[id] = ch
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		delete(w.pending, id)
		w.mu.Unlock()
	}()

	if err := sendFrame(t, op, id, payload); err != nil {
		return nil, err
	}
	select {
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		return r.data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(20 * time.Second):
		return nil, &APIError{Code: "timeout", Message: "WS 请求超时"}
	}
}

func sendFrame(t *frameConn, op, id string, payload []byte) error {
	frame := map[string]any{"op": op, "id": id}
	if payload != nil {
		frame["data"] = json.RawMessage(payload)
	}
	b, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	return t.WriteText(b)
}

func (w *wsManager) sendRaw(t *frameConn, op string, data any, id string) error {
	frame := map[string]any{"op": op}
	if id != "" {
		frame["id"] = id
	}
	if data != nil {
		frame["data"] = data
	}
	b, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	return t.WriteText(b)
}

func (w *wsManager) deliverReply(f *wsFrame) {
	w.mu.Lock()
	ch := w.pending[f.ID]
	delete(w.pending, f.ID)
	w.mu.Unlock()
	if ch == nil {
		return
	}
	reply := wsReply{data: f.Data}
	if f.Error != nil {
		f.Error.HTTPStatus = 0
		reply = wsReply{err: f.Error}
	}
	select {
	case ch <- reply:
	default:
	}
}

func (w *wsManager) failPending(cause error) {
	w.mu.Lock()
	pending := w.pending
	w.pending = map[string]chan wsReply{}
	w.mu.Unlock()
	for _, ch := range pending {
		select {
		case ch <- wsReply{err: &APIError{Code: "unavailable", Message: cause.Error()}}:
		default:
		}
	}
}

// authHandshake 连接建立后的首次 auth：只等 reply/close。
func authHandshake(t *frameConn, payload []byte, timeout time.Duration) (json.RawMessage, error) {
	type result struct {
		data json.RawMessage
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		for {
			p, err := t.ReadText(time.Now().Add(timeout))
			if err != nil {
				ch <- result{err: err}
				return
			}
			var f wsFrame
			if json.Unmarshal(p, &f) != nil {
				continue
			}
			if f.Type == "reply" && f.ID == "auth" {
				if f.Error != nil {
					ch <- result{err: f.Error}
				} else {
					ch <- result{data: f.Data}
				}
				return
			}
			// auth 阶段的其他帧（pong 等）忽略
		}
	}()
	if err := sendFrame(t, "auth", "auth", payload); err != nil {
		return nil, err
	}
	r := <-ch
	return r.data, r.err
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func sleepCtx(stop <-chan struct{}, d time.Duration) bool {
	if stop == nil {
		time.Sleep(d)
		return true
	}
	select {
	case <-stop:
		return false
	case <-time.After(d):
		return true
	}
}

// bgCtx 客户端后台 context（不受调用方约束）。
func (c *Client) bgCtx() context.Context { return context.Background() }
