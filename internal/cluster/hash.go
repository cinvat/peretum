package cluster

import (
	"hash/crc32"
	"sort"
	"sync"
	"sync/atomic"
)

// ConsistentHash implements consistent hashing with virtual nodes
// for distributing tenants across edge nodes.
type ConsistentHash struct {
	mu       sync.RWMutex
	replicas int
	hashRing []uint32          // sorted hash values
	hashMap  map[uint32]string // hash -> node name
	nodes    map[string]bool   // node name -> exists
	weights  map[string]int    // node name -> weight
}

func NewConsistentHash(replicas int) *ConsistentHash {
	if replicas <= 0 {
		replicas = 150
	}
	return &ConsistentHash{
		replicas: replicas,
		hashMap:  make(map[uint32]string),
		nodes:    make(map[string]bool),
		weights:  make(map[string]int),
	}
}

// AddNode adds a node to the hash ring with the given weight.
func (ch *ConsistentHash) AddNode(name string, weight int) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.nodes[name] {
		return // already exists
	}
	ch.nodes[name] = true
	ch.weights[name] = weight

	virtualNodes := ch.replicas * weight
	for i := 0; i < virtualNodes; i++ {
		hash := crc32.ChecksumIEEE([]byte(name + "#" + string(rune(i))))
		ch.hashRing = append(ch.hashRing, hash)
		ch.hashMap[hash] = name
	}
	sort.Slice(ch.hashRing, func(i, j int) bool {
		return ch.hashRing[i] < ch.hashRing[j]
	})
}

// RemoveNode removes a node from the hash ring.
func (ch *ConsistentHash) RemoveNode(name string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if !ch.nodes[name] {
		return
	}
	delete(ch.nodes, name)
	delete(ch.weights, name)

	weight := ch.weights[name]
	virtualNodes := ch.replicas * weight
	newRing := make([]uint32, 0, len(ch.hashRing)-virtualNodes)
	for _, h := range ch.hashRing {
		if ch.hashMap[h] != name {
			newRing = append(newRing, h)
		} else {
			delete(ch.hashMap, h)
		}
	}
	ch.hashRing = newRing
}

// GetNode returns the node responsible for the given key.
func (ch *ConsistentHash) GetNode(key string) string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.hashRing) == 0 {
		return ""
	}

	hash := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(ch.hashRing), func(i int) bool {
		return ch.hashRing[i] >= hash
	})
	if idx == len(ch.hashRing) {
		idx = 0
	}
	return ch.hashMap[ch.hashRing[idx]]
}

// GetNodes returns N distinct nodes for replication (e.g., for failover).
func (ch *ConsistentHash) GetNodes(key string, count int) []string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.hashRing) == 0 || count <= 0 {
		return nil
	}

	hash := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(ch.hashRing), func(i int) bool {
		return ch.hashRing[i] >= hash
	})

	result := make([]string, 0, count)
	seen := make(map[string]bool)
	for i := 0; i < len(ch.hashRing) && len(result) < count; i++ {
		pos := (idx + i) % len(ch.hashRing)
		node := ch.hashMap[ch.hashRing[pos]]
		if !seen[node] {
			seen[node] = true
			result = append(result, node)
		}
	}
	return result
}

// Nodes returns all node names in the ring.
func (ch *ConsistentHash) Nodes() []string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	result := make([]string, 0, len(ch.nodes))
	for n := range ch.nodes {
		result = append(result, n)
	}
	return result
}

// NodeWeight returns the weight of a node.
func (ch *ConsistentHash) NodeWeight(name string) int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return ch.weights[name]
}

// AtomicCounter is a thread-safe counter for metrics.
type AtomicCounter struct {
	value atomic.Uint64
}

func (ac *AtomicCounter) Inc() {
	ac.value.Add(1)
}

func (ac *AtomicCounter) Add(n uint64) {
	ac.value.Add(n)
}

func (ac *AtomicCounter) Get() uint64 {
	return ac.value.Load()
}

func (ac *AtomicCounter) Reset() {
	ac.value.Store(0)
}

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
