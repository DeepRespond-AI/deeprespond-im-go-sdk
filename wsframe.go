package im

// 零依赖 RFC6455 WebSocket 客户端（文本帧）。服务端 SDK 不依赖第三方库，
// 客户端 SDK 保持同样风格：握手 + 帧编解码手写实现。

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// wsCloseError 连接被对端关闭。
type wsCloseError struct {
	Code       int
	Reason     string
	RetryAfter float64 // 服务端建议的重试等待秒数（disconnect 帧）
	Payload    []byte  // close 帧原始 payload（可含服务端 JSON details）
}

func (e *wsCloseError) Error() string {
	return fmt.Sprintf("im: ws 连接关闭 code=%d reason=%s", e.Code, e.Reason)
}

// frameConn 一条 WebSocket 文本帧连接。
type frameConn struct {
	conn    net.Conn
	br      *bufio.Reader
	writeMu sync.Mutex
	closed  bool
	closeMu sync.Mutex
}

// dialFrame 建立 WS 连接并完成握手。子协议固定 im.v1.json。
func dialFrame(rawURL string, timeout time.Duration) (*frameConn, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("im: ws 地址无效: %w", err)
	}
	var address, scheme string
	switch u.Scheme {
	case "ws":
		scheme = "http"
		if u.Port() == "" {
			address = net.JoinHostPort(u.Hostname(), "80")
		} else {
			address = u.Host
		}
	case "wss":
		scheme = "https"
		if u.Port() == "" {
			address = net.JoinHostPort(u.Hostname(), "443")
		} else {
			address = u.Host
		}
	default:
		return nil, fmt.Errorf("im: 不支持的 ws 协议 %q", u.Scheme)
	}

	dialer := &net.Dialer{Timeout: timeout}
	var conn net.Conn
	if scheme == "https" {
		conn, err = tls.DialWithDialer(dialer, "tcp", address, &tls.Config{ServerName: u.Hostname()})
	} else {
		conn, err = dialer.Dial("tcp", address)
	}
	if err != nil {
		return nil, err
	}

	key := base64.StdEncoding.EncodeToString(randomBytes(16))
	req := &bytes.Buffer{}
	fmt.Fprintf(req, "GET %s HTTP/1.1\r\n", u.RequestURI())
	req.WriteString("Host: " + u.Host + "\r\n")
	req.WriteString("Upgrade: websocket\r\nConnection: Upgrade\r\n")
	req.WriteString("Sec-WebSocket-Key: " + key + "\r\n")
	req.WriteString("Sec-WebSocket-Version: 13\r\n")
	req.WriteString("Sec-WebSocket-Protocol: im.v1.json\r\n")
	req.WriteString("\r\n")
	conn.SetDeadline(time.Now().Add(timeout))
	if _, err := conn.Write(req.Bytes()); err != nil {
		conn.Close()
		return nil, err
	}

	br := bufio.NewReaderSize(conn, 4096)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet, URL: u})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("im: ws 握手响应无效: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		conn.Close()
		return nil, fmt.Errorf("im: ws 握手失败 status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if !strings.EqualFold(resp.Header.Get("Upgrade"), "websocket") ||
		!tokenListContains(resp.Header.Get("Connection"), "upgrade") ||
		resp.Header.Get("Sec-WebSocket-Accept") != acceptKey(key) {
		conn.Close()
		return nil, errors.New("im: ws 握手校验失败")
	}
	if !tokenListContains(resp.Header.Get("Sec-WebSocket-Protocol"), "im.v1.json") {
		conn.Close()
		return nil, errors.New("im: 服务端未接受 im.v1.json 子协议")
	}
	conn.SetDeadline(time.Time{})
	return &frameConn{conn: conn, br: br}, nil
}

// ReadText 读下一条文本消息 payload（自动响应 ping、忽略 pong/分片）。
// 连接被关闭时返回 *wsCloseError。
func (w *frameConn) ReadText(deadline time.Time) ([]byte, error) {
	w.conn.SetReadDeadline(deadline)
	for {
		fin, opcode, payload, err := w.readFrame()
		if err != nil {
			return nil, err
		}
		switch opcode {
		case 0x1: // text
			if !fin {
				return nil, errors.New("im: 不支持的消息分片")
			}
			return payload, nil
		case 0x8: // close
			code, reason := parseClosePayload(payload)
			_ = w.writeFrame(0x8, payload) // 回显 close
			w.closeRaw()
			return nil, &wsCloseError{Code: code, Reason: reason, Payload: payload}
		case 0x9: // ping → pong
			_ = w.writeFrame(0xA, payload)
		case 0xA, 0x0, 0x2: // pong / continuation / binary：忽略
		default:
			return nil, fmt.Errorf("im: 未知 ws 帧类型 %d", opcode)
		}
	}
}

// WriteText 发送文本帧（客户端帧必须掩码）。
func (w *frameConn) WriteText(payload []byte) error {
	return w.writeFrame(0x1, payload)
}

// CloseHandshake 发送 close 帧并等待对端回应，最终关闭 TCP。
func (w *frameConn) CloseHandshake(code int, reason string) {
	w.closeMu.Lock()
	defer w.closeMu.Unlock()
	if w.closed {
		return
	}
	payload := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(payload, uint16(code))
	copy(payload[2:], reason)
	_ = w.writeFrame(0x8, payload)
	_ = w.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, _, _ = w.readFrame() // 等 close 回包，超时不等了
	w.closeLocked()
}

func (w *frameConn) closeRaw() {
	w.closeMu.Lock()
	defer w.closeMu.Unlock()
	w.closeLocked()
}

// closeLocked 直接关闭（调用方须已持有 closeMu；CloseHandshake 内部用它避免自锁）。
func (w *frameConn) closeLocked() {
	if !w.closed {
		w.closed = true
		w.conn.Close()
	}
}

// writeFrame 写一帧（加掩码）。
func (w *frameConn) writeFrame(opcode byte, payload []byte) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	if w.closed {
		return errors.New("im: ws 已关闭")
	}
	mask := randomBytes(4)
	header := make([]byte, 0, 14)
	header = append(header, 0x80|opcode)
	n := len(payload)
	switch {
	case n < 126:
		header = append(header, byte(0x80|n))
	case n <= 0xFFFF:
		header = append(header, 0x80|126, 0, 0)
		binary.BigEndian.PutUint16(header[2:], uint16(n))
	default:
		header = append(header, 0x80|127, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(header[2:], uint64(n))
	}
	header = append(header, mask...)
	buf := make([]byte, len(header)+n)
	copy(buf, header)
	copy(buf[len(header):], payload)
	for i := 0; i < n; i++ {
		buf[len(header)+i] ^= mask[i&3]
	}
	w.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := w.conn.Write(buf)
	return err
}

// readFrame 读一帧并解掩码（服务端帧不掩码）。
func (w *frameConn) readFrame() (fin bool, opcode byte, payload []byte, err error) {
	var h [2]byte
	if _, err = io.ReadFull(w.br, h[:]); err != nil {
		return
	}
	fin = h[0]&0x80 != 0
	opcode = h[0] & 0x0F
	masked := h[1]&0x80 != 0
	length := uint64(h[1] & 0x7F)
	switch length {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(w.br, ext[:]); err != nil {
			return
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(w.br, ext[:]); err != nil {
			return
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	if length > 64<<20 {
		err = fmt.Errorf("im: ws 帧过大 %d", length)
		return
	}
	var maskKey [4]byte
	if masked {
		if _, err = io.ReadFull(w.br, maskKey[:]); err != nil {
			return
		}
	}
	payload = make([]byte, length)
	if _, err = io.ReadFull(w.br, payload); err != nil {
		return
	}
	if masked {
		for i := range payload {
			payload[i] ^= maskKey[i&3]
		}
	}
	return
}

func parseClosePayload(p []byte) (int, string) {
	if len(p) >= 2 {
		code := int(binary.BigEndian.Uint16(p[:2]))
		return code, string(p[2:])
	}
	return 1005, ""
}

func acceptKey(key string) string {
	h := sha1.Sum([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(h[:])
}

func tokenListContains(header, token string) bool {
	for _, part := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}
