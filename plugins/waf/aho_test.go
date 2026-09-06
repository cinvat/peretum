package waf

import (
	"reflect"
	"testing"
)

func TestAhoMatcherOverlap(t *testing.T) {
	// "ushers" contains "she" (index 1), "he" (0) and "hers" (3); the "he"
	// and "hers" matches overlap the "she" match. Order is not significant.
	m := buildAhoMatcher([]string{"he", "she", "his", "hers"},
		[][]int{{0}, {1}, {2}, {3}})
	got := m.match([]byte("ushers"))
	if !sameSet(got, []int{0, 1, 3}) {
		t.Fatalf("match(ushers) = %v, want {0 1 3}", got)
	}
}

func sameSet(a, b []int) bool {
	set := make(map[int]struct{}, len(a))
	for _, v := range a {
		set[v] = struct{}{}
	}
	if len(set) != len(b) {
		return false
	}
	for _, v := range b {
		if _, ok := set[v]; !ok {
			return false
		}
	}
	return true
}

func TestAhoMatcherDedupAndEmpty(t *testing.T) {
	// Pattern "abc" (cond 0 and 1 share it), pattern "bc" (cond 2), "cab" (cond 3).
	m := buildAhoMatcher([]string{"abc", "abc", "bc", "cab", ""},
		[][]int{{0}, {1}, {2}, {3}, {4}})
	got := m.match([]byte("xxcababcxx"))
	// "cab" (3), "abc" (0 and 1), "bc" (2) all fire; empty pattern skipped.
	want := []int{3, 0, 1, 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("match = %v, want %v", got, want)
	}
}

func TestAhoMatcherAllEmpty(t *testing.T) {
	if m := buildAhoMatcher([]string{"", ""}, [][]int{{0}, {1}}); m != nil {
		t.Fatal("expected nil for all-empty patterns")
	}
}

func TestAhoMatcherNoMatch(t *testing.T) {
	m := buildAhoMatcher([]string{"zzz", "yyy"}, [][]int{{0}, {1}})
	if got := m.match([]byte("abcxyz")); len(got) != 0 {
		t.Fatalf("expected no matches, got %v", got)
	}
}

func TestAhoMatcherMatchDedup(t *testing.T) {
	// The same pattern matching at multiple positions is reported once.
	m := buildAhoMatcher([]string{"ab"}, [][]int{{7}})
	got := m.match([]byte("abxxab"))
	// cond 7 should fire exactly once.
	if !reflect.DeepEqual(got, []int{7}) {
		t.Fatalf("match(abxxab) = %v, want [7]", got)
	}
}

func TestAhoMatcherNil(t *testing.T) {
	var m *ahoMatcher
	if got := m.match([]byte("x")); got != nil {
		t.Fatalf("nil matcher match = %v", got)
	}
	m2 := buildAhoMatcher([]string{"a"}, [][]int{{0}})
	if got := m2.match(nil); got != nil {
		t.Fatalf("empty haystack match = %v", got)
	}
}
