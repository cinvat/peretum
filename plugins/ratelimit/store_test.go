package ratelimit

import "testing"

func TestStoreAllow(t *testing.T) {
	s := NewStore()
	key := "rule1|1.2.3.4"
	allowed := 0
	for i := 0; i < 5; i++ {
		if s.Allow(key, 2, 2) {
			allowed++
		}
	}
	if allowed != 2 {
		t.Fatalf("expected 2 allowed with burst 2, got %d", allowed)
	}
	if !s.Allow("other|key", 2, 2) {
		t.Fatalf("different key should have its own bucket")
	}
}
