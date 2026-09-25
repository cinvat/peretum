package cluster

import (
	"sync"
	"testing"
)

func TestLRUCapacityAndLen(t *testing.T) {
	c := NewLRUCache[string, int](3)
	if c.Capacity() != 3 {
		t.Fatalf("Capacity = %d, want 3", c.Capacity())
	}
	if c.Len() != 0 {
		t.Fatalf("Len on a new cache = %d, want 0", c.Len())
	}

	// A non-positive capacity falls back to a default rather than misbehaving.
	if got := NewLRUCache[string, int](0).Capacity(); got <= 0 {
		t.Fatalf("default capacity = %d, want a positive default", got)
	}
}

func TestLRUGetPutDelete(t *testing.T) {
	c := NewLRUCache[string, string](4)

	if _, ok := c.Get("missing"); ok {
		t.Error("Get on a missing key should report false")
	}

	c.Put("a", "1")
	c.Put("b", "2")
	if v, ok := c.Get("a"); !ok || v != "1" {
		t.Fatalf("Get(a) = %q, %v", v, ok)
	}
	if c.Len() != 2 {
		t.Fatalf("Len = %d, want 2", c.Len())
	}

	// Overwriting updates in place rather than adding an entry.
	c.Put("a", "1-updated")
	if c.Len() != 2 {
		t.Fatalf("Len after overwrite = %d, want 2", c.Len())
	}
	if v, _ := c.Get("a"); v != "1-updated" {
		t.Fatalf("Get(a) after overwrite = %q, want 1-updated", v)
	}

	if !c.Delete("a") {
		t.Error("Delete of an existing key should report true")
	}
	if c.Delete("a") {
		t.Error("Delete of a missing key should report false")
	}
	if _, ok := c.Get("a"); ok {
		t.Error("a should be gone after Delete")
	}
}

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	c := NewLRUCache[string, int](2)
	c.Put("a", 1)
	c.Put("b", 2)
	c.Put("c", 3) // capacity reached; "a" is the least recently used

	if _, ok := c.Get("a"); ok {
		t.Error("a should have been evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Error("b should still be present")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("c should be present")
	}
	if c.Len() != 2 {
		t.Fatalf("Len = %d, want 2", c.Len())
	}
}

func TestLRUGetRefreshesRecency(t *testing.T) {
	c := NewLRUCache[string, int](2)
	c.Put("a", 1)
	c.Put("b", 2)

	// Reading "a" makes "b" the least recently used, so "b" is evicted next.
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should be present")
	}
	c.Put("c", 3)

	if _, ok := c.Get("b"); ok {
		t.Error("b should have been evicted after a refreshed it")
	}
	if _, ok := c.Get("a"); !ok {
		t.Error("a should have been kept because it was just read")
	}
}

func TestLRUConcurrentAccess(t *testing.T) {
	c := NewLRUCache[int, int](64)

	const workers = 16
	const perWorker = 200
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				key := w*perWorker + i
				c.Put(key, key)
				c.Get(key)
				if i%3 == 0 {
					c.Delete(key)
				}
				c.Len()
			}
		}(w)
	}
	wg.Wait()

	if c.Len() > c.Capacity() {
		t.Fatalf("Len = %d exceeds capacity %d", c.Len(), c.Capacity())
	}
}
