// Package realtime holds services' realtime connections. The runner keeps the
// connections, not the service: a service subscribes a request to channels,
// then publishes to a channel or sends to one connection, and receives what
// WebSocket clients send through its message() handler.
package realtime

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	MaxChannels     = 32
	MaxChannelBytes = 256
	queueLen        = 256
)

// Event is what a service's message() handler receives.
type Event struct {
	Type       string          `json:"type"` // message or close
	Connection string          `json:"connection"`
	Channels   []string        `json:"channels"`
	Meta       json.RawMessage `json:"meta"`
	Data       string          `json:"data,omitempty"`
}

// Handler delivers an event to a service.
type Handler func(ctx context.Context, serviceID string, e Event) error

type conn struct {
	id        string
	serviceID string
	channels  []string
	meta      json.RawMessage
	out       chan string
	done      chan struct{}
	once      sync.Once
}

func (c *conn) closeOut() { c.once.Do(func() { close(c.done) }) }

type Hub struct {
	handler Handler

	mu         sync.Mutex
	channels   map[string]map[*conn]struct{} // serviceID + "\x00" + channel
	conns      map[string]*conn              // id
	perService map[string]int
}

func NewHub(h Handler) *Hub {
	return &Hub{handler: h, channels: map[string]map[*conn]struct{}{}, conns: map[string]*conn{}, perService: map[string]int{}}
}

// Subscription is what the service's subscribe() returned.
type Subscription struct {
	Channels []string        `json:"channels"`
	Meta     json.RawMessage `json:"meta"`
}

// ParseSubscription validates the x-zeroko-realtime header of a response.
func ParseSubscription(header string) (*Subscription, bool) {
	var s Subscription
	if json.Unmarshal([]byte(header), &s) != nil || len(s.Channels) == 0 || len(s.Channels) > MaxChannels {
		return nil, false
	}
	for _, c := range s.Channels {
		if c == "" || len(c) > MaxChannelBytes || strings.ContainsRune(c, 0) {
			return nil, false
		}
	}
	if len(s.Meta) == 0 {
		s.Meta = json.RawMessage("null")
	}
	return &s, true
}

func (h *Hub) add(c *conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.conns[c.id] = c
	h.perService[c.serviceID]++
	for _, ch := range c.channels {
		k := c.serviceID + "\x00" + ch
		if h.channels[k] == nil {
			h.channels[k] = map[*conn]struct{}{}
		}
		h.channels[k][c] = struct{}{}
	}
}

func (h *Hub) remove(c *conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conns[c.id] != c {
		return
	}
	delete(h.conns, c.id)
	if h.perService[c.serviceID]--; h.perService[c.serviceID] <= 0 {
		delete(h.perService, c.serviceID)
	}
	for _, ch := range c.channels {
		k := c.serviceID + "\x00" + ch
		delete(h.channels[k], c)
		if len(h.channels[k]) == 0 {
			delete(h.channels, k)
		}
	}
}

// enqueue hands data to a connection; one that cannot keep up is dropped.
func enqueue(c *conn, data string) bool {
	select {
	case c.out <- data:
		return true
	default:
		c.closeOut()
		return false
	}
}

// Publish sends data to every connection of a service subscribed to channel and
// returns how many there were.
func (h *Hub) Publish(serviceID, channel, data string) int {
	h.mu.Lock()
	targets := make([]*conn, 0, len(h.channels[serviceID+"\x00"+channel]))
	for c := range h.channels[serviceID+"\x00"+channel] {
		targets = append(targets, c)
	}
	h.mu.Unlock()
	n := 0
	for _, c := range targets {
		if enqueue(c, data) {
			n++
		}
	}
	return n
}

// Send sends data to one connection of a service.
func (h *Hub) Send(serviceID, id, data string) bool {
	h.mu.Lock()
	c := h.conns[id]
	h.mu.Unlock()
	if c == nil || c.serviceID != serviceID {
		return false
	}
	return enqueue(c, data)
}

// closed forgets a connection and tells its service.
func (h *Hub) closed(c *conn) {
	h.remove(c)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.handler(ctx, c.serviceID, h.event("close", c, ""))
}

// Member is a connection on a channel, as its service sees it.
type Member struct {
	Connection string          `json:"connection"`
	Meta       json.RawMessage `json:"meta"`
}

// MaxMembers is how many connections Members lists.
const MaxMembers = 1000

// Members lists the connections of a service on a channel.
func (h *Hub) Members(serviceID, channel string) []Member {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := []Member{}
	for c := range h.channels[serviceID+"\x00"+channel] {
		if len(out) == MaxMembers {
			break
		}
		out = append(out, Member{c.id, c.meta})
	}
	return out
}

// Connections is the number of open connections of a service.
func (h *Hub) Connections(serviceID string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.perService[serviceID]
}

// CloseAll closes every connection.
func (h *Hub) CloseAll() { h.CloseService("") }

// CloseService closes every connection of a service (of every service, for "").
func (h *Hub) CloseService(serviceID string) {
	h.mu.Lock()
	var cs []*conn
	for _, c := range h.conns {
		if serviceID == "" || c.serviceID == serviceID {
			cs = append(cs, c)
		}
	}
	h.mu.Unlock()
	for _, c := range cs {
		c.closeOut()
	}
}

// Serve turns the request into a realtime connection: a WebSocket if the
// client asked for one, otherwise Server-Sent Events. It returns when the
// connection closes.
func (h *Hub) Serve(w http.ResponseWriter, r *http.Request, serviceID string, s *Subscription) {
	c := &conn{
		id:        "c_" + strings.ToLower(rand.Text()),
		serviceID: serviceID,
		channels:  s.Channels,
		meta:      s.Meta,
		out:       make(chan string, queueLen),
		done:      make(chan struct{}),
	}
	if IsWebSocket(r) {
		h.serveWS(w, r, c)
	} else {
		h.serveSSE(w, r, c)
	}
}

func (h *Hub) event(typ string, c *conn, data string) Event {
	return Event{Type: typ, Connection: c.id, Channels: c.channels, Meta: c.meta, Data: data}
}

func (h *Hub) serveWS(w http.ResponseWriter, r *http.Request, c *conn) {
	ws, err := acceptWS(w, r)
	if err != nil {
		return
	}
	h.add(c)
	defer h.closed(c)

	readErr := make(chan struct{})
	go func() {
		defer close(readErr)
		for {
			_, msg, err := ws.read()
			if err != nil {
				return
			}
			// One message at a time per connection, in order.
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			h.handler(ctx, c.serviceID, h.event("message", c, string(msg)))
			cancel()
		}
	}()

	ping := time.NewTicker(30 * time.Second)
	defer ping.Stop()
	for {
		select {
		case data := <-c.out:
			if ws.write(opText, []byte(data)) != nil {
				ws.conn.Close()
				<-readErr
				return
			}
		case <-ping.C:
			ws.write(opPing, nil)
		case <-c.done:
			ws.close(1000)
			<-readErr
			return
		case <-readErr:
			ws.conn.Close()
			return
		}
	}
}

func (h *Hub) serveSSE(w http.ResponseWriter, r *http.Request, c *conn) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming is not supported", http.StatusInternalServerError)
		return
	}
	hd := w.Header()
	hd.Set("Content-Type", "text/event-stream")
	hd.Set("Cache-Control", "no-store")
	hd.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	h.add(c)
	defer h.closed(c)
	rc := http.NewResponseController(w)
	keep := time.NewTicker(25 * time.Second)
	defer keep.Stop()
	write := func(s string) bool {
		rc.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err := w.Write([]byte(s)); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	for {
		select {
		case data := <-c.out:
			var b strings.Builder
			for _, line := range strings.Split(data, "\n") {
				b.WriteString("data: ")
				b.WriteString(strings.TrimSuffix(line, "\r"))
				b.WriteString("\n")
			}
			b.WriteString("\n")
			if !write(b.String()) {
				return
			}
		case <-keep.C:
			if !write(": keepalive\n\n") {
				return
			}
		case <-c.done:
			return
		case <-r.Context().Done():
			return
		}
	}
}
