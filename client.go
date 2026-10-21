package im

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Config 客户端配置。
type Config struct {
	AppKey string // 组织#应用，如 "org#myapp"
	APIURL string // REST 基址，如 "http://127.0.0.1:18480"

	WSURL        string        // 可选：WS 地址；缺省由 APIURL 推导 http→ws + /client/v1/ws
	DeviceID     string        // 可选：设备 ID；缺省启动时生成并保存在 Client 内
	DeviceName   string        // 可选：设备名，缺省 "go"
	Platform     string        // 可选：平台标识（ios/android/web/...），缺省 "go"
	SDKVersion   string        // 可选：缺省 "1.0.0-alpha"
	OSVersion    string        // 可选
	AppVersion   string        // 可选
	HTTPTimeout  time.Duration // 可选：REST 超时，缺省 15s
	TokenStorage TokenStorage  // 可选：令牌持久化（进程内默认内存）；nil 表示不持久化
	Logger       Logger        // 可选：日志接收器；nil 表示不打日志
}

// TokenStorage 令牌持久化接口（重启后自动恢复登录）。实现需自行保证并发安全。
type TokenStorage interface {
	Load(appKey string) (*Session, error)
	Save(appKey string, s *Session) error
	Clear(appKey string) error
}

// Logger 日志接口。Level 为 debug/info/warn/error。
type Logger interface {
	Log(level, module, message string, fields map[string]any)
}

// Client IM 客户端。一个实例对应一个登录用户（多账号创建多个实例）。
// REST 方法可直接调用；Connect() 建立长连接后才能收事件与用 WS 发消息。
type Client struct {
	cfg    Config
	http   *http.Client
	wsBase string

	mu        sync.RWMutex
	session   *Session
	connected bool
	ws        *wsManager
	store     *Store

	syncRetried bool // SyncAll 对 reset/recent_required 的重入保护
	log         Logger
}

// New 创建客户端。不做任何网络请求。
func New(cfg Config) (*Client, error) {
	if cfg.AppKey == "" || !strings.Contains(cfg.AppKey, "#") {
		return nil, fmt.Errorf("im: AppKey 必须为 组织#应用 形式")
	}
	cfg.APIURL = strings.TrimRight(cfg.APIURL, "/")
	if cfg.APIURL == "" {
		return nil, fmt.Errorf("im: APIURL 必填")
	}
	if cfg.WSURL == "" {
		cfg.WSURL = strings.Replace(cfg.APIURL, "http", "ws", 1) + "/client/v1/ws"
	}
	if cfg.DeviceID == "" {
		cfg.DeviceID = randomHex(16)
	}
	if cfg.DeviceName == "" {
		cfg.DeviceName = "go"
	}
	if cfg.Platform == "" {
		cfg.Platform = "go"
	}
	if cfg.SDKVersion == "" {
		cfg.SDKVersion = "1.0.0-alpha"
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 15 * time.Second
	}
	c := &Client{
		cfg:    cfg,
		http:   &http.Client{Timeout: cfg.HTTPTimeout},
		wsBase: cfg.APIURL,
		store:  newStore(),
		log:    cfg.Logger,
	}
	if cfg.TokenStorage != nil {
		if s, err := cfg.TokenStorage.Load(cfg.AppKey); err == nil && s != nil {
			c.session = s
		}
	}
	return c, nil
}

// AppKey 返回配置的应用标识。
func (c *Client) AppKey() string { return c.cfg.AppKey }

// CurrentUser 返回当前登录用户名；未登录返回空串。
func (c *Client) CurrentUser() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.session != nil && c.session.User != nil {
		return c.session.User.Username
	}
	return ""
}

// Store 返回内存会话/消息缓存（长连接开启后自动维护）。
func (c *Client) Store() *Store { return c.store }

// SessionID 返回当前登录会话的服务端会话 ID（未登录时为空）。
// 可用于设备管理界面标记"本机"或对照 Sessions 列表。
func (c *Client) SessionID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.session != nil {
		return c.session.SessionID
	}
	return ""
}

func (c *Client) token() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.session == nil {
		return ""
	}
	return c.session.AccessToken
}

func (c *Client) refreshToken() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.session == nil {
		return ""
	}
	return c.session.RefreshToken
}

func (c *Client) setSession(s *Session) {
	c.mu.Lock()
	c.session = s
	c.mu.Unlock()
	if c.cfg.TokenStorage != nil {
		_ = c.cfg.TokenStorage.Save(c.cfg.AppKey, s)
	}
}

// ---- 登录 ----

// 登录方式。
const (
	LoginPassword = "password"
	LoginTicket   = "ticket"
)

// Login 登录。grantType 为 LoginPassword（username+password）或 LoginTicket（username+ticket，
// ticket 由服务端控制口签发，用于免密托管登录）。
func (c *Client) Login(ctx context.Context, grantType, username, secret string) (*Session, error) {
	body := map[string]any{
		"app_key":     c.cfg.AppKey,
		"grant_type":  grantType,
		"username":    username,
		"device_id":   c.cfg.DeviceID,
		"device_name": c.cfg.DeviceName,
		"platform":    c.cfg.Platform,
		"sdk_version": c.cfg.SDKVersion,
	}
	switch grantType {
	case LoginPassword:
		body["password"] = secret
	case LoginTicket:
		body["ticket"] = secret
	default:
		return nil, fmt.Errorf("im: 不支持的登录方式 %q", grantType)
	}
	var s Session
	if err := c.rawRequest(ctx, http.MethodPost, "/client/v1/sessions", body, false, &s); err != nil {
		return nil, err
	}
	c.setSession(&s)
	c.store.setSelf(username)
	return &s, nil
}

// Refresh 用 refresh token 续期。
func (c *Client) Refresh(ctx context.Context) error {
	rt := c.refreshToken()
	if rt == "" {
		return fmt.Errorf("im: 未登录，无法续期")
	}
	var s Session
	if err := c.rawRequest(ctx, http.MethodPost, "/client/v1/sessions/refresh", map[string]any{"refresh_token": rt}, false, &s); err != nil {
		// 会话彻底失效（refresh token 也过期/吊销）时清空本地状态
		if IsCode(err, CodeUnauthenticated) {
			c.clearSession()
		}
		return err
	}
	c.setSession(&s)
	return nil
}

// Logout 退出登录并断开长连接。
func (c *Client) Logout(ctx context.Context) error {
	c.Disconnect()
	err := c.rawRequest(ctx, http.MethodPost, "/client/v1/sessions/logout", nil, false, nil)
	c.clearSession()
	return err
}

func (c *Client) clearSession() {
	c.mu.Lock()
	c.session = nil
	c.mu.Unlock()
	if c.cfg.TokenStorage != nil {
		_ = c.cfg.TokenStorage.Clear(c.cfg.AppKey)
	}
}

// ---- REST 核心 ----

// request 发送已认证的 REST 请求；遇 401/unauthenticated 自动续期重试一次。
func (c *Client) request(ctx context.Context, method, path string, body, out any) error {
	err := c.rawRequest(ctx, method, path, body, true, out)
	if err == nil || !IsCode(err, CodeUnauthenticated) || c.refreshToken() == "" {
		return err
	}
	if apiErr, ok := err.(*APIError); ok && apiErr.Reason() != "" && apiErr.Reason() != "token_expired" && apiErr.Reason() != "token_invalid" {
		return err // 会话被吊销等，不自动续期
	}
	if rerr := c.Refresh(ctx); rerr != nil {
		return err // 返回原始 401
	}
	return c.rawRequest(ctx, method, path, body, true, out)
}

// rawRequest 发送原始请求。withAuth 为 true 时带 Authorization 头。
func (c *Client) rawRequest(ctx context.Context, method, path string, body any, withAuth bool, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("im: 编码请求体: %w", err)
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.APIURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if withAuth {
		if tok := c.token(); tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		apiErr := &APIError{HTTPStatus: resp.StatusCode, Code: fmt.Sprintf("http_%d", resp.StatusCode)}
		var e struct {
			Error *APIError `json:"error"`
		}
		if json.Unmarshal(data, &e) == nil && e.Error != nil {
			apiErr.Code, apiErr.Message, apiErr.Details, apiErr.RequestID =
				e.Error.Code, e.Error.Message, e.Error.Details, e.Error.RequestID
		} else if len(data) > 0 {
			apiErr.Message = strings.TrimSpace(string(data))
		}
		return apiErr
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

// getJSON 带 query 的 GET。
func (c *Client) getJSON(ctx context.Context, path string, query url.Values, out any) error {
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	return c.request(ctx, http.MethodGet, path, nil, out)
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// newClientMsgID 生成客户端消息 ID（≤64 可见 ASCII）。
func newClientMsgID() string { return randomHex(16) }

func (c *Client) logf(level, module, msg string, fields map[string]any) {
	if c.log != nil {
		c.log.Log(level, module, msg, fields)
	}
}
