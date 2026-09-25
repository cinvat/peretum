package loadbalancer

import (
	"sync"
	"testing"
	"time"
)

func TestAtomicTimeRoundTrip(t *testing.T) {
	var at AtomicTime

	if !at.IsZero() {
		t.Fatal("zero value should report IsZero")
	}
	if got := at.Load(); !got.IsZero() {
		t.Fatalf("Load on zero value = %v, want zero time", got)
	}
	if got := at.LoadNano(); got != 0 {
		t.Fatalf("LoadNano on zero value = %d, want 0", got)
	}

	want := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	at.Store(want)

	if at.IsZero() {
		t.Fatal("after Store, IsZero should be false")
	}
	if got := at.Load(); !got.Equal(want) {
		t.Fatalf("Load = %v, want %v", got, want)
	}
	if got := at.LoadNano(); got != want.UnixNano() {
		t.Fatalf("LoadNano = %d, want %d", got, want.UnixNano())
	}

	// Storing the zero time must clear it back to the zero value.
	at.Store(time.Time{})
	if !at.IsZero() {
		t.Fatal("Store(time.Time{}) should reset to the zero value")
	}
	if got := at.Load(); !got.IsZero() {
		t.Fatalf("Load after reset = %v, want zero time", got)
	}
}

func TestAtomicTimeConcurrentAccess(t *testing.T) {
	// The point of AtomicTime is that MarkHealthy in a request goroutine can
	// write while probeCandidate reads. Under -race this is the real assertion.
	var at AtomicTime
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				at.Store(time.Unix(0, int64(j)))
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = at.Load()
				_ = at.LoadNano()
				_ = at.IsZero()
			}
		}()
	}
	wg.Wait()
}
