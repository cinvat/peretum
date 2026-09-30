package ratelimit

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Store is a shared token-bucket registry keyed by an arbitrary string
// (client IP, target|IP, rule ID, ...). Both the standalone ratelimit
// plugin and the WAF rate_limit action delegate to it so limits share
// the same accounting.
type Store struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	params   map[string]bucketParams
	lastSeen map[string]time.Time
	ttl      time.Duration
}

type bucketParams struct {
	rps   float64
	burst int
}

func NewStore() *Store {
	return &Store{
		limiters: make(map[string]*rate.Limiter),
		params:   make(map[string]bucketParams),
		lastSeen: make(map[string]time.Time),
		ttl:      5 * time.Minute,
	}
}

// Allow reports whether one event for key may proceed under rps/burst.
// Buckets are created lazily; a bucket created with different params is
// replaced so rule config changes take effect without a restart.
func (s *Store) Allow(key string, rps float64, burst int) bool {
	if rps <= 0 {
		rps = 100
	}
	if burst <= 0 {
		burst = 200
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.limiters[key]
	if !ok || s.params[key] != (bucketParams{rps, burst}) {
		l = rate.NewLimiter(rate.Limit(rps), burst)
		s.limiters[key] = l
		s.params[key] = bucketParams{rps, burst}
	}
	s.lastSeen[key] = time.Now()
	allowed := l.Allow()
	if len(s.limiters)%1024 == 0 {
		for k, t := range s.lastSeen {
			if time.Since(t) > s.ttl {
				delete(s.limiters, k)
				delete(s.params, k)
				delete(s.lastSeen, k)
			}
		}
	}
	return allowed
}
