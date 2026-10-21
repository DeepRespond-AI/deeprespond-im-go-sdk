package im

// wsframe 单测：RFC6455 帧编解码回环（三种长度边界、掩码）、acceptKey 已知向量、
// close 载荷解析、ping/pong 自动回应、close 握手、dialFrame 握手校验。

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// tcpPair 建一条真实 TCP 连接，两端各包一个 frameConn：
// client 侧写掩码帧读明文帧，server 侧写明文帧读掩码帧。
func tcpPair(t *testing.T) (client, server *frameConn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	type accepted struct {
		c   net.Conn
		err error
	}
	ch := make(chan accepted, 1)
	go func() {
		c, err := ln.Accept()
		ch <- accepted{c, err}
	}()
	dialed, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	acc := <-ch
	if acc.err != nil {
		t.Fatalf("accept: %v", acc.err)
	}
	client = &frameConn{conn: dialed, br: bufio.NewReaderSize(dialed, 4096)}
	server = &frameConn{conn: acc.c, br: bufio.NewReaderSize(acc.c, 4096)}
	t.Cleanup(func() { client.closeRaw(); server.closeRaw() })
	return client, server
}

func TestAcceptKeyRFCVector(t *testing.T) {
	// RFC 6455 §1.3 示例
	const key = "dGhlIHNhbXBsZSBub25jZQ=="
	const want = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
	if got := acceptKey(key); got != want {
		t.Fatalf("acceptKey = %q, want %q", got, want)
	}
}

func TestTokenListContains(t *testing.T) {
	cases := []struct {
		header, token string
		want          bool
	}{
		{"Upgrade", "upgrade", true},
		{"keep-alive, Upgrade", "upgrade", true},
		{"Upgrade,keep-alive", "upgrade", true},
		{"keep-alive", "upgrade", false},
		{"", "upgrade", false},
	}
	for _, tc := range cases {
		if got := tokenListContains(tc.header, tc.token); got != tc.want {
			t.Errorf("tokenListContains(%q, %q) = %v, want %v", tc.header, tc.token, got, tc.want)
		}
	}
}

func TestParseClosePayload(t *testing.T) {
	if code, reason := parseClosePayload([]byte{0x03, 0xE8, 'b', 'y', 'e'}); code != 1000 || reason != "bye" {
		t.Fatalf("code=%d reason=%q, want 1000/bye", code, reason)
	}
	if code, _ := parseClosePayload(nil); code != 1005 {
		t.Fatalf("空 payload code=%d, want 1005", code)
	}
	if code, _ := parseClosePayload([]byte{0x03}); code != 1005 {
		t.Fatalf("单字节 payload code=%d, want 1005", code)
	}
}

func TestFrameRoundtrip(t *testing.T) {
	sizes := []int{0, 5, 125, 126, 300, 65535, 65536, 70000}
	for _, size := range sizes {
		t.Run(fmtSize(size), func(t *testing.T) {
			client, server := tcpPair(t)
			payload := make([]byte, size)
			_, _ = rand.Read(payload)

			done := make(chan error, 1)
			var got []byte
			var opcode byte
			var fin bool
			go func() {
				f, op, p, err := server.readFrame()
				fin, opcode, got = f, op, p
				done <- err
			}()
			if err := client.writeFrame(0x1, payload); err != nil {
				t.Fatalf("writeFrame: %v", err)
			}
			if err := <-done; err != nil {
				t.Fatalf("readFrame: %v", err)
			}
			if !fin || opcode != 0x1 {
				t.Fatalf("fin=%v opcode=%d, want true/1", fin, opcode)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("size=%d payload 不一致（掩码编解码有误）", size)
			}
		})
	}
}

func fmtSize(n int) string {
	switch {
	case n < 1024:
		return "short"
	case n <= 0xFFFF:
		return "mid"
	default:
		return "large"
	}
}

func TestReadTextAutoPong(t *testing.T) {
	client, server := tcpPair(t)
	// 服务端先发 ping 再发 text：客户端 ReadText 应自动回 pong 并返回 text
	go func() {
		_ = server.writeFrame(0x9, []byte("hb"))    // ping
		_ = server.writeFrame(0x1, []byte("hello")) // text
	}()
	deadline := time.Now().Add(5 * time.Second)
	got, err := client.ReadText(deadline)
	if err != nil {
		t.Fatalf("ReadText: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("ReadText = %q, want hello", got)
	}
	// 服务端应收到客户端回的 pong
	fin, op, p, err := server.readFrame()
	if err != nil || op != 0xA || !fin || string(p) != "hb" {
		t.Fatalf("pong 回包异常: op=%d payload=%q err=%v", op, p, err)
	}
}

func TestReadTextCloseFrame(t *testing.T) {
	client, server := tcpPair(t)
	go func() {
		payload := []byte{0x03, 0xE8, 'b', 'y', 'e'} // 1000 bye
		_ = server.writeFrame(0x8, payload)
	}()
	_, err := client.ReadText(time.Now().Add(5 * time.Second))
	closeErr, ok := err.(*wsCloseError)
	if !ok {
		t.Fatalf("应返回 *wsCloseError, got %v", err)
	}
	if closeErr.Code != 1000 || closeErr.Reason != "bye" {
		t.Fatalf("code=%d reason=%q, want 1000/bye", closeErr.Code, closeErr.Reason)
	}
	// 客户端应已回显 close 并关 TCP
	_, op, _, err := server.readFrame()
	if err != nil || op != 0x8 {
		t.Fatalf("服务端应收到 close 回显: op=%d err=%v", op, err)
	}
}

func TestCloseHandshake(t *testing.T) {
	client, server := tcpPair(t)
	errCh := make(chan error, 1)
	go func() {
		// 服务端读到 close 后回一个 close，完成双向握手
		_, op, p, err := server.readFrame()
		if err != nil || op != 0x8 {
			errCh <- err
			return
		}
		code, reason := parseClosePayload(p)
		if code != 1000 || reason != "bye" {
			errCh <- &wsCloseError{Code: code, Reason: reason}
			return
		}
		errCh <- server.writeFrame(0x8, p)
	}()
	client.CloseHandshake(1000, "bye")
	if err := <-errCh; err != nil {
		t.Fatalf("close 握手异常: %v", err)
	}
	// close 握手后客户端连接应已关闭
	client.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, _, err := client.readFrame(); err == nil {
		t.Fatal("close 后再读应报错")
	}
}

// wsTestServer 模拟 WS 握手端：校验请求头后按 wantProto 决定是否回子协议，
// 握手成功后把服务端侧 frameConn 送回供测试交互。
func wsTestServer(t *testing.T, wantProto bool, status int) (*httptest.Server, <-chan *frameConn) {
	t.Helper()
	connCh := make(chan *frameConn, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Sec-WebSocket-Key")
		if status != http.StatusSwitchingProtocols || key == "" ||
			r.Header.Get("Sec-WebSocket-Protocol") != "im.v1.json" {
			w.WriteHeader(status)
			_, _ = w.Write([]byte("no ws here"))
			return
		}
		w.Header().Set("Upgrade", "websocket")
		w.Header().Set("Connection", "Upgrade")
		w.Header().Set("Sec-WebSocket-Accept", acceptKey(key))
		if wantProto {
			w.Header().Set("Sec-WebSocket-Protocol", "im.v1.json")
		}
		w.WriteHeader(http.StatusSwitchingProtocols)
		nc, brw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		connCh <- &frameConn{conn: nc, br: brw.Reader}
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, connCh
}

func wsURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/client/v1/ws"
}

func TestDialFrameHandshake(t *testing.T) {
	srv, connCh := wsTestServer(t, true, http.StatusSwitchingProtocols)
	fc, err := dialFrame(wsURL(srv), 5*time.Second)
	if err != nil {
		t.Fatalf("dialFrame: %v", err)
	}
	defer fc.closeRaw()

	// 双向通一条消息
	server := <-connCh
	go func() { _ = server.WriteText([]byte(`{"type":"pong"}`)) }()
	got, err := fc.ReadText(time.Now().Add(5 * time.Second))
	if err != nil || string(got) != `{"type":"pong"}` {
		t.Fatalf("ReadText = %q err=%v", got, err)
	}
	if err := fc.WriteText([]byte(`{"op":"ping"}`)); err != nil {
		t.Fatalf("WriteText: %v", err)
	}
	fin, op, p, err := server.readFrame()
	if err != nil || op != 0x1 || !fin || !bytes.Equal(p, []byte(`{"op":"ping"}`)) {
		t.Fatalf("服务端收帧异常: op=%d payload=%s err=%v", op, p, err)
	}
}

func TestDialFrameRejectsMissingSubprotocol(t *testing.T) {
	srv, _ := wsTestServer(t, false, http.StatusSwitchingProtocols)
	_, err := dialFrame(wsURL(srv), 5*time.Second)
	if err == nil || !strings.Contains(err.Error(), "子协议") {
		t.Fatalf("应报子协议错误, got %v", err)
	}
}

func TestDialFrameRejectsHTTPStatus(t *testing.T) {
	srv, _ := wsTestServer(t, true, http.StatusBadRequest)
	_, err := dialFrame(wsURL(srv), 5*time.Second)
	if err == nil || !strings.Contains(err.Error(), "握手失败 status=400") {
		t.Fatalf("应报握手失败 400, got %v", err)
	}
}

func TestDialFrameRejectsBadScheme(t *testing.T) {
	if _, err := dialFrame("ftp://example.com/ws", time.Second); err == nil {
		t.Fatal("非 ws/wss 协议应报错")
	}
}
