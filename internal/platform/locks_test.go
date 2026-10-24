package platform

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestLockTurns(t *testing.T) {
	p := &Platform{}
	ctx := context.Background()
	first, err := p.Lock(ctx, "s", "DB", "k")
	if err != nil {
		t.Fatal(err)
	}
	// Another key is free; the same key in another binding or service too.
	for _, k := range [][3]string{{"s", "DB", "other"}, {"s", "OTHER", "k"}, {"t", "DB", "k"}} {
		id, err := p.Lock(ctx, k[0], k[1], k[2])
		if err != nil {
			t.Fatal(err)
		}
		p.Unlock(k[0], k[1], k[2], id)
	}

	var mu sync.Mutex
	var order []int
	var wg sync.WaitGroup
	for i := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := p.Lock(ctx, "s", "DB", "k")
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			p.Unlock("s", "DB", "k", id)
		}()
		time.Sleep(20 * time.Millisecond) // queue in order
	}
	mu.Lock()
	if len(order) != 0 {
		t.Fatal("a waiter ran while the lock was held")
	}
	mu.Unlock()
	p.Unlock("s", "DB", "k", "not the holder")
	p.Unlock("s", "DB", "k", first)
	wg.Wait()
	if len(order) != 3 || order[0] != 0 || order[1] != 1 || order[2] != 2 {
		t.Fatalf("turns %v, want [0 1 2]", order)
	}
	if len(p.keyLocks.held) != 0 {
		t.Fatalf("locks left: %v", p.keyLocks.held)
	}
}

func TestLockGivesUpWhenTheCallerDoes(t *testing.T) {
	p := &Platform{}
	id, _ := p.Lock(context.Background(), "s", "DB", "k")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := p.Lock(ctx, "s", "DB", "k"); err == nil {
		t.Fatal("got a held lock")
	}
	p.Unlock("s", "DB", "k", id)
	if len(p.keyLocks.held) != 0 {
		t.Fatal("the waiter that left still holds a place")
	}
}
