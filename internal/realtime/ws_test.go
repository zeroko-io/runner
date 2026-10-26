package realtime

import (
	"bufio"
	"io"
	"net"
	"testing"
)

// frame is a client's masked frame (a zero mask: the payload as it is).
func frame(fin bool, op byte, payload []byte) []byte {
	b := []byte{op, 0x80 | byte(len(payload))}
	if fin {
		b[0] |= 0x80
	}
	if len(payload) > 125 {
		b[1] = 0x80 | 126
		b = append(b, byte(len(payload)>>8), byte(len(payload)))
	}
	return append(append(b, 0, 0, 0, 0), payload...)
}

func TestReadFrames(t *testing.T) {
	cases := []struct {
		name   string
		frames [][]byte
		ok     bool
	}{
		{"text", [][]byte{frame(true, opText, []byte("hi"))}, true},
		{"fragmented, a ping between", [][]byte{frame(false, opText, []byte("h")), frame(true, opPing, nil), frame(true, opContinuation, []byte("i"))}, true},
		{"unmasked", [][]byte{{0x81, 0x02, 'h', 'i'}}, false},
		{"long ping", [][]byte{frame(true, opPing, make([]byte, 126))}, false},
		{"fragmented ping", [][]byte{frame(false, opPing, nil)}, false},
		{"continuation first", [][]byte{frame(true, opContinuation, []byte("x"))}, false},
	}
	for _, c := range cases {
		server, client := net.Pipe()
		go func() {
			for _, f := range c.frames {
				client.Write(f)
			}
		}()
		// Pongs and closes the server writes go nowhere.
		go io.Copy(io.Discard, client)
		ws := &wsConn{conn: server, br: bufio.NewReader(server)}
		_, msg, err := ws.read()
		if (err == nil) != c.ok || (c.ok && string(msg) != "hi") {
			t.Errorf("%s: %q, %v", c.name, msg, err)
		}
		server.Close()
		client.Close()
	}
}
