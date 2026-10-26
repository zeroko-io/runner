package realtime

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// A minimal server side of RFC 6455: text and binary messages, fragmentation,
// ping/pong and close. No extensions.

const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xa

	maxMessage = 1 << 20
)

var errTooLarge = errors.New("message too large")

// IsWebSocket reports whether r asks to upgrade to a WebSocket.
func IsWebSocket(r *http.Request) bool {
	return r.Method == http.MethodGet && headerHas(r.Header, "Connection", "upgrade") && headerHas(r.Header, "Upgrade", "websocket")
}

func headerHas(h http.Header, name, token string) bool {
	for _, v := range h.Values(name) {
		for _, t := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(t), token) {
				return true
			}
		}
	}
	return false
}

type wsConn struct {
	conn net.Conn
	br   *bufio.Reader
	wmu  sync.Mutex
}

func acceptWS(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" || r.Header.Get("Sec-WebSocket-Version") != "13" {
		http.Error(w, "unsupported WebSocket version", http.StatusBadRequest)
		return nil, errors.New("bad handshake")
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "WebSockets are not supported here", http.StatusInternalServerError)
		return nil, errors.New("cannot hijack")
	}
	conn, brw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}
	h := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	resp := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " +
		base64.StdEncoding.EncodeToString(h[:]) + "\r\n\r\n"
	conn.SetDeadline(time.Time{})
	if _, err := conn.Write([]byte(resp)); err != nil {
		conn.Close()
		return nil, err
	}
	return &wsConn{conn: conn, br: brw.Reader}, nil
}

// read returns the next data message, answering pings on the way.
func (c *wsConn) read() (op byte, msg []byte, err error) {
	for {
		fin, opcode, payload, err := c.readFrame()
		if err != nil {
			return 0, nil, err
		}
		switch opcode {
		case opPing:
			c.write(opPong, payload)
			continue
		case opPong:
			continue
		case opClose:
			c.write(opClose, payload)
			return 0, nil, io.EOF
		case opText, opBinary:
			if op != 0 {
				return 0, nil, errors.New("new message inside a fragmented one")
			}
			op = opcode
		case opContinuation:
			if op == 0 {
				return 0, nil, errors.New("continuation without a message")
			}
		default:
			return 0, nil, errors.New("unknown opcode")
		}
		if len(msg)+len(payload) > maxMessage {
			c.close(1009)
			return 0, nil, errTooLarge
		}
		msg = append(msg, payload...)
		if fin {
			return op, msg, nil
		}
	}
}

func (c *wsConn) readFrame() (fin bool, op byte, payload []byte, err error) {
	var h [2]byte
	if _, err = io.ReadFull(c.br, h[:]); err != nil {
		return
	}
	fin, op = h[0]&0x80 != 0, h[0]&0x0f
	if h[0]&0x70 != 0 {
		return false, 0, nil, errors.New("reserved bits set")
	}
	if h[1]&0x80 == 0 {
		return false, 0, nil, errors.New("client frames must be masked")
	}
	n := uint64(h[1] & 0x7f)
	switch n {
	case 126:
		var b [2]byte
		if _, err = io.ReadFull(c.br, b[:]); err != nil {
			return
		}
		n = uint64(binary.BigEndian.Uint16(b[:]))
	case 127:
		var b [8]byte
		if _, err = io.ReadFull(c.br, b[:]); err != nil {
			return
		}
		n = binary.BigEndian.Uint64(b[:])
	}
	if n > maxMessage {
		c.close(1009)
		return false, 0, nil, errTooLarge
	}
	// Control frames are whole and short (RFC 6455, 5.5): a ping's payload
	// comes back in its pong.
	if op >= opClose && (n > 125 || !fin) {
		c.close(1002)
		return false, 0, nil, errors.New("control frames are at most 125 bytes, unfragmented")
	}
	var mask [4]byte
	if _, err = io.ReadFull(c.br, mask[:]); err != nil {
		return
	}
	payload = make([]byte, n)
	if _, err = io.ReadFull(c.br, payload); err != nil {
		return
	}
	for i := range payload {
		payload[i] ^= mask[i%4]
	}
	return fin, op, payload, nil
}

func (c *wsConn) write(op byte, payload []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	var h [10]byte
	h[0] = 0x80 | op
	n := len(payload)
	hl := 2
	switch {
	case n < 126:
		h[1] = byte(n)
	case n <= 0xffff:
		h[1] = 126
		binary.BigEndian.PutUint16(h[2:], uint16(n))
		hl = 4
	default:
		h[1] = 127
		binary.BigEndian.PutUint64(h[2:], uint64(n))
		hl = 10
	}
	c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.conn.Write(append(h[:hl:hl], payload...)); err != nil {
		return err
	}
	return nil
}

func (c *wsConn) close(code uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], code)
	c.write(opClose, b[:])
	c.conn.Close()
}
