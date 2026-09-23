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

	if _, _, ok, err := ts.GetTargetByHost(ctx, "missing"); err != nil || ok {
		t.Fatalf("GetTargetByHost(missing) = ok %v, err %v", ok, err)
	}
	if _, ok, err := ts.GetGlobal(ctx); err != nil || ok {
		t.Fatalf("GetGlobal(missing) = ok %v, err %v", ok, err)
	}

	yaml := []byte("name: t\nlisten: t\nupstreams:\n  - url: http://x:1\n")
	if err := ts.PutTargetByHost(ctx, "t", "v1", yaml); err != nil {
		t.Fatalf("PutTargetByHost: %v", err)
	}
	if err := ts.PutGlobal(ctx, []byte(`{"listeners":[":8080"]}`)); err != nil {
		t.Fatalf("PutGlobal: %v", err)
	}

	if version, got, ok, err := ts.GetTargetByHost(ctx, "t"); err != nil || !ok {
		t.Fatalf("GetTargetByHost(t) = ok %v, err %v", ok, err)
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

	hosts, err := ts.ListHosts(ctx)
	if err != nil {
		t.Fatalf("ListHosts: %v", err)
	}
	if len(hosts) != 1 || hosts["t"] != "v1" {
		t.Fatalf("ListHosts = %v", hosts)
	}

	if err := ts.DeleteTargetByHost(ctx, "t"); err != nil {
		t.Fatalf("DeleteTargetByHost: %v", err)
	}
	if _, _, ok, _ := ts.GetTargetByHost(ctx, "t"); ok {
		t.Fatal("target t should be deleted")
	}
	hosts, _ = ts.ListHosts(ctx)
	if len(hosts) != 0 {
		t.Fatalf("ListHosts after delete = %v", hosts)
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
	if err := ts.PutTargetByHost(ctx, "a", "v1", []byte("name: a\nlisten: a")); err != nil {
		t.Fatalf("PutTargetByHost: %v", err)
	}
	if err := ts.PutTargetByHost(ctx, "b", "v2", []byte("name: b\nlisten: b")); err != nil {
		t.Fatalf("PutTargetByHost: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ts2, err := OpenTargetStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ts2.Close()

	hosts, err := ts2.ListHosts(ctx)
	if err != nil {
		t.Fatalf("ListHosts: %v", err)
	}
	if len(hosts) != 2 || hosts["a"] != "v1" || hosts["b"] != "v2" {
		t.Fatalf("ListHosts after reopen = %v", hosts)
	}
	count, err := ts2.HostCount(ctx)
	if err != nil || count != 2 {
		t.Fatalf("HostCount = %d, err %v", count, err)
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
		if err := ts.PutTargetByHost(ctx, uniqueName(i), "v", []byte("{}")); err != nil {
			t.Fatalf("PutTargetByHost(%d): %v", i, err)
		}
	}
	hosts, err := ts.ListHosts(ctx)
	if err != nil {
		t.Fatalf("ListHosts: %v", err)
	}
	if len(hosts) != n {
		t.Fatalf("ListHosts = %d names, want %d", len(hosts), n)
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
			if err := ts.PutTargetByHost(ctx, name, "v1", []byte("{}")); err != nil {
				t.Errorf("PutTargetByHost(%d): %v", i, err)
			}
			if _, _, ok, err := ts.GetTargetByHost(ctx, name); err != nil || !ok {
				t.Errorf("GetTargetByHost(%d): ok=%v err=%v", i, ok, err)
			}
		}(i)
	}
	wg.Wait()

	hosts, err := ts.ListHosts(ctx)
	if err != nil {
		t.Fatalf("ListHosts: %v", err)
	}
	if len(hosts) != n {
		t.Fatalf("ListHosts = %d names, want %d", len(hosts), n)
	}
}

func uniqueName(i int) string {
	return "target_" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('0'+i%10))
}
