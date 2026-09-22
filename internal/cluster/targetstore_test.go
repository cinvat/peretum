package cluster

import (
	"context"
	"sync"
	"testing"
)

func TestTargetStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	ts, err := OpenTargetStore(dir)
	if err != nil {
		t.Fatalf("OpenTargetStore: %v", err)
	}

	if _, _, ok, err := ts.GetTarget(ctx, "missing"); err != nil || ok {
		t.Fatalf("GetTarget(missing) = ok %v, err %v", ok, err)
	}
	if _, ok, err := ts.GetGlobal(ctx); err != nil || ok {
		t.Fatalf("GetGlobal(missing) = ok %v, err %v", ok, err)
	}

	yaml := []byte("name: t\nupstreams:\n  - url: http://x:1\n")
	if err := ts.PutTarget(ctx, "t", "v1", yaml); err != nil {
		t.Fatalf("PutTarget: %v", err)
	}
	if err := ts.PutGlobal(ctx, []byte(`{"listeners":[":8080"]}`)); err != nil {
		t.Fatalf("PutGlobal: %v", err)
	}

	if version, got, ok, err := ts.GetTarget(ctx, "t"); err != nil || !ok {
		t.Fatalf("GetTarget(t) = ok %v, err %v", ok, err)
	} else if version != "v1" {
		t.Fatalf("version = %q, want v1", version)
	} else if string(got) != string(yaml) {
		t.Fatalf("data = %q, want %q", got, yaml)
	}

	if got, ok, err := ts.GetGlobal(ctx); err != nil || !ok {
		t.Fatalf("GetGlobal = ok %v, err %v", ok, err)
	} else if string(got) != `{"listeners":[":8080"]}` {
		t.Fatalf("global = %q", got)
	}

	names, err := ts.ListTargets(ctx)
	if err != nil {
		t.Fatalf("ListTargets: %v", err)
	}
	if len(names) != 1 || names["t"] != "v1" {
		t.Fatalf("ListTargets = %v", names)
	}

	if err := ts.DeleteTarget(ctx, "t"); err != nil {
		t.Fatalf("DeleteTarget: %v", err)
	}
	if _, _, ok, _ := ts.GetTarget(ctx, "t"); ok {
		t.Fatal("target t should be deleted")
	}
	names, _ = ts.ListTargets(ctx)
	if len(names) != 0 {
		t.Fatalf("ListTargets after delete = %v", names)
	}

	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestTargetStoreReopenPersists(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	ts, err := OpenTargetStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := ts.PutTarget(ctx, "a", "v1", []byte("name: a")); err != nil {
		t.Fatalf("PutTarget: %v", err)
	}
	if err := ts.PutTarget(ctx, "b", "v2", []byte("name: b")); err != nil {
		t.Fatalf("PutTarget: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ts2, err := OpenTargetStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ts2.Close()

	names, err := ts2.ListTargets(ctx)
	if err != nil {
		t.Fatalf("ListTargets: %v", err)
	}
	if len(names) != 2 || names["a"] != "v1" || names["b"] != "v2" {
		t.Fatalf("ListTargets after reopen = %v", names)
	}
	count, err := ts2.TargetCount(ctx)
	if err != nil || count != 2 {
		t.Fatalf("TargetCount = %d, err %v", count, err)
	}
}

func TestTargetStoreManyNames(t *testing.T) {
	ctx := context.Background()
	ts, err := OpenTargetStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenTargetStore: %v", err)
	}
	defer ts.Close()

	const n = 500
	for i := 0; i < n; i++ {
		if err := ts.PutTarget(ctx, uniqueName(i), "v", []byte("{}")); err != nil {
			t.Fatalf("PutTarget(%d): %v", i, err)
		}
	}
	names, err := ts.ListTargets(ctx)
	if err != nil {
		t.Fatalf("ListTargets: %v", err)
	}
	if len(names) != n {
		t.Fatalf("ListTargets = %d names, want %d", len(names), n)
	}
}

func TestTargetStoreConcurrent(t *testing.T) {
	ctx := context.Background()
	ts, err := OpenTargetStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenTargetStore: %v", err)
	}
	defer ts.Close()

	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := uniqueName(i)
			if err := ts.PutTarget(ctx, name, "v1", []byte("{}")); err != nil {
				t.Errorf("PutTarget(%d): %v", i, err)
			}
			if _, _, ok, err := ts.GetTarget(ctx, name); err != nil || !ok {
				t.Errorf("GetTarget(%d): ok=%v err=%v", i, ok, err)
			}
		}(i)
	}
	wg.Wait()

	names, err := ts.ListTargets(ctx)
	if err != nil {
		t.Fatalf("ListTargets: %v", err)
	}
	if len(names) != n {
		t.Fatalf("ListTargets = %d names, want %d", len(names), n)
	}
}

func uniqueName(i int) string {
	return "target_" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('0'+i%10))
}
