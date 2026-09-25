package cluster

import "sync"

// LRUCache is a simple LRU cache with bounded size.
// Used for caching resolved tenant configs.
type LRUCache[K comparable, V any] struct {
	mu       sync.Mutex
	capacity int
	cache    map[K]*lruNode[K, V]
	head     *lruNode[K, V]
	tail     *lruNode[K, V]
}

type lruNode[K comparable, V any] struct {
	key   K
	value V
	prev  *lruNode[K, V]
	next  *lruNode[K, V]
}

func NewLRUCache[K comparable, V any](capacity int) *LRUCache[K, V] {
	if capacity <= 0 {
		capacity = 10000
	}
	head := &lruNode[K, V]{}
	tail := &lruNode[K, V]{}
	head.next = tail
	tail.prev = head
	return &LRUCache[K, V]{
		capacity: capacity,
		cache:    make(map[K]*lruNode[K, V], capacity),
		head:     head,
		tail:     tail,
	}
}

func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.cache[key]
	if !ok {
		var zero V
		return zero, false
	}
	c.moveToFront(node)
	return node.value, true
}

func (c *LRUCache[K, V]) Put(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if node, ok := c.cache[key]; ok {
		node.value = value
		c.moveToFront(node)
		return
	}

	if len(c.cache) >= c.capacity {
		c.evict()
	}

	node := &lruNode[K, V]{key: key, value: value}
	c.cache[key] = node
	c.addToFront(node)
}

func (c *LRUCache[K, V]) Delete(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.cache[key]
	if !ok {
		return false
	}
	c.remove(node)
	delete(c.cache, key)
	return true
}

func (c *LRUCache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.cache)
}

func (c *LRUCache[K, V]) Capacity() int {
	return c.capacity
}

func (c *LRUCache[K, V]) addToFront(node *lruNode[K, V]) {
	node.next = c.head.next
	node.prev = c.head
	c.head.next.prev = node
	c.head.next = node
}

func (c *LRUCache[K, V]) remove(node *lruNode[K, V]) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

func (c *LRUCache[K, V]) moveToFront(node *lruNode[K, V]) {
	c.remove(node)
	c.addToFront(node)
}

func (c *LRUCache[K, V]) evict() {
	node := c.tail.prev
	if node == c.head {
		return
	}
	c.remove(node)
	delete(c.cache, node.key)
}
