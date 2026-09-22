package loadbalancer

import (
	"hash/crc32"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Upstream struct {
	URL       string
	Weight    int
	Healthy   atomic.Bool
	LastCheck AtomicTime
}

// AtomicTime lets an upstream's LastCheck be updated concurrently (MarkHealthy
// in a request goroutine) while probeCandidate reads it without racing.
type AtomicTime struct {
	v atomic.Int64 // unix nanoseconds; 0 means the zero time
}

func (at *AtomicTime) Store(t time.Time) {
	var n int64
	if !t.IsZero() {
		n = t.UnixNano()
	}
	at.v.Store(n)
}

func (at *AtomicTime) Load() time.Time {
	if n := at.v.Load(); n != 0 {
		return time.Unix(0, n)
	}
	return time.Time{}
}

func (at *AtomicTime) IsZero() bool {
	return at.v.Load() == 0
}

type LoadBalancer interface {
	Next(r *http.Request) *Upstream
	MarkHealthy(url string, healthy bool)
	GetUpstreams() []*Upstream
}

// probeCandidate returns the upstream whose health has not been confirmed
// for the longest time (oldest LastCheck). When every upstream is
// unhealthy, Next falls back to this candidate so a dead upstream is
// retried by subsequent requests and can be marked healthy again once it
// recovers. Without this, an upstream that failed once would never be
// selected again and would stay unhealthy until the process restarted.
func probeCandidate(upstreams []*Upstream) *Upstream {
	var candidate *Upstream
	for _, u := range upstreams {
		if candidate == nil || u.LastCheck.Load().Before(candidate.LastCheck.Load()) {
			candidate = u
		}
	}
	return candidate
}

type roundRobin struct {
	upstreams []*Upstream
	idx       atomic.Uint64
}

func NewRoundRobin(upstreams []*Upstream) LoadBalancer {
	for _, u := range upstreams {
		u.Healthy.Store(true)
	}
	return &roundRobin{upstreams: upstreams}
}

func (rr *roundRobin) Next(r *http.Request) *Upstream {
	n := len(rr.upstreams)
	if n == 0 {
		return nil
	}

	for i := 0; i < n; i++ {
		idx := rr.idx.Add(1) - 1
		u := rr.upstreams[idx%uint64(n)]
		if u.Healthy.Load() {
			return u
		}
	}
	return probeCandidate(rr.upstreams)
}

func (rr *roundRobin) MarkHealthy(url string, healthy bool) {
	for _, u := range rr.upstreams {
		if u.URL == url {
			u.Healthy.Store(healthy)
			u.LastCheck.Store(time.Now())
			return
		}
	}
}

func (rr *roundRobin) GetUpstreams() []*Upstream {
	return rr.upstreams
}

type weightedRoundRobin struct {
	upstreams []*Upstream
	weights   []int
	current   int
	total     int
	mu        sync.Mutex
}

func NewWeightedRoundRobin(upstreams []*Upstream) LoadBalancer {
	var weights []int
	total := 0
	for _, u := range upstreams {
		w := u.Weight
		if w <= 0 {
			w = 1
		}
		weights = append(weights, w)
		total += w
		u.Healthy.Store(true)
	}
	return &weightedRoundRobin{
		upstreams: upstreams,
		weights:   weights,
		total:     total,
	}
}

func (wrr *weightedRoundRobin) Next(r *http.Request) *Upstream {
	wrr.mu.Lock()
	defer wrr.mu.Unlock()

	n := len(wrr.upstreams)
	if n == 0 {
		return nil
	}

	for i := 0; i < n; i++ {
		wrr.current = (wrr.current + 1) % n
		u := wrr.upstreams[wrr.current]
		if u.Healthy.Load() && wrr.weights[wrr.current] > 0 {
			wrr.weights[wrr.current]--
			if wrr.weights[wrr.current] == 0 {
				for j := 0; j < n; j++ {
					if wrr.upstreams[j].Healthy.Load() {
						wrr.weights[j] = wrr.upstreams[j].Weight
						if wrr.weights[j] <= 0 {
							wrr.weights[j] = 1
						}
					}
				}
			}
			return u
		}
	}
	return probeCandidate(wrr.upstreams)
}

func (wrr *weightedRoundRobin) MarkHealthy(url string, healthy bool) {
	for _, u := range wrr.upstreams {
		if u.URL == url {
			u.Healthy.Store(healthy)
			u.LastCheck.Store(time.Now())
			return
		}
	}
}

func (wrr *weightedRoundRobin) GetUpstreams() []*Upstream {
	return wrr.upstreams
}

const maglevTableSize = 65537

type maglev struct {
	upstreams []*Upstream
	table     []int
	mu        sync.RWMutex
}

func NewMaglev(upstreams []*Upstream) LoadBalancer {
	for _, u := range upstreams {
		u.Healthy.Store(true)
	}
	m := &maglev{upstreams: upstreams}
	m.buildTable()
	return m
}

func (m *maglev) buildTable() {
	m.mu.Lock()
	defer m.mu.Unlock()

	n := len(m.upstreams)
	m.table = make([]int, maglevTableSize)

	permutations := make([][]uint64, n)
	for i, u := range m.upstreams {
		h1 := crc32.ChecksumIEEE([]byte(u.URL + "1"))
		h2 := crc32.ChecksumIEEE([]byte(u.URL + "2"))
		perm := make([]uint64, maglevTableSize)
		for j := range perm {
			perm[j] = (uint64(h1) + uint64(j)*uint64(h2)) % uint64(maglevTableSize)
		}
		permutations[i] = perm
	}

	filled := 0
	next := make([]int, n)
	for filled < maglevTableSize {
		for i := 0; i < n; i++ {
			if filled >= maglevTableSize {
				break
			}
			u := m.upstreams[i]
			if !u.Healthy.Load() {
				continue
			}
			for {
				idx := permutations[i][next[i]] % uint64(maglevTableSize)
				next[i]++
				if m.table[idx] == 0 {
					m.table[idx] = i
					filled++
					break
				}
			}
		}
	}
}

func (m *maglev) Next(r *http.Request) *Upstream {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.upstreams) == 0 {
		return nil
	}

	hash := crc32.ChecksumIEEE([]byte(r.URL.Path + r.URL.RawQuery))
	idx := m.table[hash%maglevTableSize]
	u := m.upstreams[idx]
	if u.Healthy.Load() {
		return u
	}

	for i := 0; i < len(m.upstreams); i++ {
		if m.upstreams[i].Healthy.Load() {
			return m.upstreams[i]
		}
	}
	return probeCandidate(m.upstreams)
}

func (m *maglev) MarkHealthy(url string, healthy bool) {
	m.mu.Lock()
	for _, u := range m.upstreams {
		if u.URL == url {
			u.Healthy.Store(healthy)
			u.LastCheck.Store(time.Now())
			break
		}
	}
	m.mu.Unlock()
	m.buildTable()
}

func (m *maglev) GetUpstreams() []*Upstream {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.upstreams
}

type leastConnections struct {
	upstreams []*Upstream
	active    []atomic.Int64
}

func NewLeastConnections(upstreams []*Upstream) LoadBalancer {
	active := make([]atomic.Int64, len(upstreams))
	for _, u := range upstreams {
		u.Healthy.Store(true)
	}
	return &leastConnections{upstreams: upstreams, active: active}
}

func (lc *leastConnections) Next(r *http.Request) *Upstream {
	var best *Upstream
	var bestActive int64 = -1

	for i, u := range lc.upstreams {
		if !u.Healthy.Load() {
			continue
		}
		curr := lc.active[i].Load()
		if bestActive == -1 || curr < bestActive {
			bestActive = curr
			best = u
		}
	}
	if best == nil {
		return probeCandidate(lc.upstreams)
	}
	return best
}

func (lc *leastConnections) MarkHealthy(url string, healthy bool) {
	for _, u := range lc.upstreams {
		if u.URL == url {
			u.Healthy.Store(healthy)
			u.LastCheck.Store(time.Now())
			return
		}
	}
}

func (lc *leastConnections) GetUpstreams() []*Upstream {
	return lc.upstreams
}

func (lc *leastConnections) Increment(url string) {
	for i, u := range lc.upstreams {
		if u.URL == url {
			lc.active[i].Add(1)
			return
		}
	}
}

func (lc *leastConnections) Decrement(url string) {
	for i, u := range lc.upstreams {
		if u.URL == url {
			lc.active[i].Add(-1)
			return
		}
	}
}

func New(algorithm string, upstreams []*Upstream) LoadBalancer {
	switch algorithm {
	case "weighted_rr", "wrr":
		return NewWeightedRoundRobin(upstreams)
	case "maglev":
		return NewMaglev(upstreams)
	case "least_conn", "least_connections":
		return NewLeastConnections(upstreams)
	case "round_robin", "rr", "":
		fallthrough
	default:
		return NewRoundRobin(upstreams)
	}
}
