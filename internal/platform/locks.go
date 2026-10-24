package platform

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/zeroko-io/runner/internal/store"
)

const (
	lockHold   = 30 * time.Second // a lock not released by then is released
	lockWait   = 30 * time.Second // the longest a caller waits for one
	maxLockKey = 256
)

// locks serialize what a service does with a key: the holder of a key's lock
// runs alone, and waiters take it in the order they asked. They live in
// memory, per service, binding and key, and are not kept across restarts.
type locks struct {
	mu   sync.Mutex
	held map[string]*lock
}

type lock struct {
	id    string
	timer *time.Timer
	queue []chan string // waiters, given the id of their turn
}

// Lock waits for the lock of key and returns the id that releases it.
func (p *Platform) Lock(ctx context.Context, serviceID, binding, key string) (string, error) {
	if key == "" || len(key) > maxLockKey {
		return "", Invalid("lock keys are 1 to 256 bytes")
	}
	l := &p.keyLocks
	k := serviceID + "\x00" + binding + "\x00" + key
	l.mu.Lock()
	if l.held == nil {
		l.held = map[string]*lock{}
	}
	h := l.held[k]
	if h == nil {
		h = &lock{}
		l.held[k] = h
		id := l.grant(k, h)
		l.mu.Unlock()
		return id, nil
	}
	turn := make(chan string, 1)
	h.queue = append(h.queue, turn)
	l.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, lockWait)
	defer cancel()
	select {
	case id := <-turn:
		return id, nil
	case <-ctx.Done():
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if i := slices.Index(h.queue, turn); i >= 0 {
		h.queue = slices.Delete(h.queue, i, i+1)
	} else {
		l.release(k, <-turn) // given the lock as the wait ended: pass it on
	}
	return "", Invalid("waited " + lockWait.String() + " for the lock of " + key)
}

// Unlock releases a lock Lock returned; an id whose time ran out does nothing.
func (p *Platform) Unlock(serviceID, binding, key, id string) {
	l := &p.keyLocks
	l.mu.Lock()
	defer l.mu.Unlock()
	l.release(serviceID+"\x00"+binding+"\x00"+key, id)
}

// grant gives h a new holder. l.mu is held.
func (l *locks) grant(k string, h *lock) string {
	id := store.NewID()
	h.id = id
	h.timer = time.AfterFunc(lockHold, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.release(k, id)
	})
	return id
}

// release passes the lock of k to the next waiter, or drops it. l.mu is held.
func (l *locks) release(k, id string) {
	h := l.held[k]
	if h == nil || h.id != id {
		return
	}
	h.timer.Stop()
	if len(h.queue) == 0 {
		delete(l.held, k)
		return
	}
	next := h.queue[0]
	h.queue = h.queue[1:]
	next <- l.grant(k, h)
}
