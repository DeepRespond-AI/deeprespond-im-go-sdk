package main

// 浏览器侧 WebSocket 桥：最小 RFC6455 服务端实现（仅文本帧 + 关闭握手），
// 与 SDK 零依赖原则一致，demo 不引入第三方库。
//
// 握手：Sec-WebSocket-Accept = base64(sha1(key + GUID))。
// 客户端帧必须带掩码；服务端下行不带掩码。分片帧浏览器几乎不用于
// JSON 消息，这里仍按规范累积处理。

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

var errBridgeClosed = errors.New("ws 桥已关闭")

// bridgeConn 一条浏览器 ⇄ 后端的 WebSocket 连接。
type bridgeConn struct {
	rwc *bufio.ReadWriter
	nc  interface{ Close() error }

	wmu    sync.Mutex
	closed bool
}

// acceptBridge 完成 HTTP → WebSocket 升级握手。
func acceptBridge(w http.ResponseWriter, r *http.Request) (*bridgeConn, error) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") ||
		!strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") {
		http.Error(w, "需要 WebSocket 升级", http.StatusBadRequest)
		return nil, errors.New("非 WebSocket 请求")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(w, "缺少 Sec-WebSocket-Key", http.StatusBadRequest)
		return nil, errors.New("缺少握手密钥")
	}
	h := sha1.New()
	_, _ = io.WriteString(h, key+wsGUID)
	accept := base64.StdEncoding.EncodeToString(h.Sum(nil))

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "连接不支持 Hijack", http.StatusInternalServerError)
		return nil, errors.New("不支持 Hijack")
	}
	nc, rwb, err := hj.Hijack()
	if err != nil {
		return nil, err
	}
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := rwb.WriteString(resp); err != nil {
		nc.Close()
		return nil, err
	}
	if err := rwb.Flush(); err != nil {
		nc.Close()
		return nil, err
	}
	return &bridgeConn{rwc: rwb, nc: nc}, nil
}

// ReadMessage 读取一条完整文本帧（自动累积分片、跳过 ping/pong）。
func (b *bridgeConn) ReadMessage() ([]byte, error) {
	var assembled []byte
	for {
		fin, opcode, payload, err := b.readFrame()
		if err != nil {
			return nil, err
		}
		switch opcode {
		case 0x8: // close
			_ = b.writeFrame(0x8, payload)
			return nil, errBridgeClosed
		case 0x9: // ping → pong
			_ = b.writeFrame(0xA, payload)
			continue
		case 0xA: // pong
			continue
		case 0x1, 0x2, 0x0: // text / binary / continuation
			assembled = append(assembled, payload...)
			if fin {
				return assembled, nil
			}
		default:
			return nil, errors.New("未知帧类型")
		}
	}
}

// readFrame 读单帧并去除客户端掩码。
func (b *bridgeConn) readFrame() (fin bool, opcode byte, payload []byte, err error) {
	head := make([]byte, 2)
	if _, err = io.ReadFull(b.rwc, head); err != nil {
		return
	}
	fin = head[0]&0x80 != 0
	opcode = head[0] & 0x0F
	masked := head[1]&0x80 != 0
	length := int64(head[1] & 0x7F)
	switch length {
	case 126:
		ext := make([]byte, 2)
		if _, err = io.ReadFull(b.rwc, ext); err != nil {
			return
		}
		length = int64(binary.BigEndian.Uint16(ext))
	case 127:
		ext := make([]byte, 8)
		if _, err = io.ReadFull(b.rwc, ext); err != nil {
			return
		}
		length = int64(binary.BigEndian.Uint64(ext))
	}
	if length > 8<<20 {
		err = errors.New("帧过大")
		return
	}
	var mask []byte
	if masked {
		mask = make([]byte, 4)
		if _, err = io.ReadFull(b.rwc, mask); err != nil {
			return
		}
	}
	payload = make([]byte, length)
	if _, err = io.ReadFull(b.rwc, payload); err != nil {
		return
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return
}

// WriteMessage 发送文本帧（无掩码，自动按 ≤1MB 分片不需要）。
func (b *bridgeConn) WriteMessage(payload []byte) error {
	b.wmu.Lock()
	defer b.wmu.Unlock()
	if b.closed {
		return errBridgeClosed
	}
	_ = b.nc.(interface{ SetWriteDeadline(time.Time) error }).
		SetWriteDeadline(time.Now().Add(10 * time.Second))
	return b.writeFrame(0x1, payload)
}

func (b *bridgeConn) writeFrame(opcode byte, payload []byte) error {
	var head []byte
	n := len(payload)
	switch {
	case n < 126:
		head = []byte{0x80 | opcode, byte(n)}
	case n < 1<<16:
		head = []byte{0x80 | opcode, 126, byte(n >> 8), byte(n)}
	default:
		head = make([]byte, 10)
		head[0] = 0x80 | opcode
		head[1] = 127
		binary.BigEndian.PutUint64(head[2:], uint64(n))
	}
	if _, err := b.rwc.Write(head); err != nil {
		return err
	}
	if _, err := b.rwc.Write(payload); err != nil {
		return err
	}
	return b.rwc.Flush()
}

// Close 发送关闭帧并释放连接。
func (b *bridgeConn) Close() {
	b.wmu.Lock()
	if b.closed {
		b.wmu.Unlock()
		return
	}
	b.closed = true
	_ = b.writeFrame(0x8, []byte{0x03, 0xE8}) // 1000
	b.rwc.Flush()
	b.wmu.Unlock()
	_ = b.nc.Close()
}
