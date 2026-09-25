package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestTargetStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	ts, err := OpenTargetStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenTargetStore: %v", err)
	}
	defer ts.Close()

	if _, ok, err := ts.GetTargetByHost(ctx, "missing"); err != nil || ok {
		t.Fatalf("GetTargetByHost(missing) = ok %v, err %v", ok, err)
	}
	if _, ok, err := ts.GetGlobal(ctx); err != nil || ok {
		t.Fatalf("GetGlobal(missing) = ok %v, err %v", ok, err)
	}

	blob := []byte("server_name: t\nupstreams:\n  - url: http://x:1\n")
	if err := ts.PutTargetByHost(ctx, "t", blob); err != nil {
		t.Fatalf("PutTargetByHost: %v", err)
	}
	if err := ts.PutGlobal(ctx, []byte(`{"listeners":[":8080"]}`)); err != nil {
		t.Fatalf("PutGlobal: %v", err)
	}

	got, ok, err := ts.GetTargetByHost(ctx, "t")
	if err != nil || !ok {
		t.Fatalf("GetTargetByHost(t) = ok %v, err %v", ok, err)
	}
	if string(got) != string(blob) {
		t.Fatalf("data = %q, want %q", got, blob)
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
	if len(hosts) != 1 || hosts[0] != "t" {
		t.Fatalf("ListHosts = %v, want [t]", hosts)
	}

	// Overwriting replaces the blob; there is no version history.
	updated := []byte("server_name: t\nupstreams:\n  - url: http://y:2\n")
	if err := ts.PutTargetByHost(ctx, "t", updated); err != nil {
		t.Fatalf("overwrite PutTargetByHost: %v", err)
	}
	if got, _, _ := ts.GetTargetByHost(ctx, "t"); string(got) != string(updated) {
		t.Fatalf("data after overwrite = %q, want %q", got, updated)
	}

	if err := ts.DeleteTargetByHost(ctx, "t"); err != nil {
		t.Fatalf("DeleteTargetByHost: %v", err)
	}
	if _, ok, _ := ts.GetTargetByHost(ctx, "t"); ok {
		t.Fatal("target t should be deleted")
	}
	if hosts, _ := ts.ListHosts(ctx); len(hosts) != 0 {
		t.Fatalf("ListHosts after delete = %v", hosts)
	}
	// Deleting a missing key is not an error.
	if err := ts.DeleteTargetByHost(ctx, "t"); err != nil {
		t.Fatalf("DeleteTargetByHost(missing): %v", err)
	}
}

func TestTargetStoreGlobalIsNotAHost(t *testing.T) {
	ctx := context.Background()
	ts, err := OpenTargetStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenTargetStore: %v", err)
	}
	defer ts.Close()

	if err := ts.PutGlobal(ctx, []byte("{}")); err != nil {
		t.Fatalf("PutGlobal: %v", err)
	}
	if err := ts.PutTargetByHost(ctx, "a", []byte("{}")); err != nil {
		t.Fatalf("PutTargetByHost: %v", err)
	}

	hosts, err := ts.ListHosts(ctx)
	if err != nil {
		t.Fatalf("ListHosts: %v", err)
	}
	if len(hosts) != 1 || hosts[0] != "a" {
		t.Fatalf("ListHosts = %v, want only [a]; the global key must not be listed", hosts)
	}
}

func TestTargetStoreReopenPersists(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	ts, err := OpenTargetStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := ts.PutTargetByHost(ctx, "a", []byte("server_name: a")); err != nil {
		t.Fatalf("PutTargetByHost: %v", err)
	}
	if err := ts.PutTargetByHost(ctx, "b", []byte("server_name: b")); err != nil {
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
	if len(hosts) != 2 {
		t.Fatalf("ListHosts after reopen = %v, want 2", hosts)
	}
	if got, ok, _ := ts2.GetTargetByHost(ctx, "b"); !ok || string(got) != "server_name: b" {
		t.Fatalf("GetTargetByHost(b) after reopen = %q ok=%v", got, ok)
	}
	if count, err := ts2.HostCount(ctx); err != nil || count != 2 {
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
		if err := ts.PutTargetByHost(ctx, fmt.Sprintf("host%d.example.com", i), []byte("{}")); err != nil {
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
			name := fmt.Sprintf("c%d.example.com", i)
			if err := ts.PutTargetByHost(ctx, name, []byte("{}")); err != nil {
				t.Errorf("PutTargetByHost(%d): %v", i, err)
			}
			if _, ok, err := ts.GetTargetByHost(ctx, name); err != nil || !ok {
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

func TestTargetStoreHasAnyTarget(t *testing.T) {
	ctx := context.Background()
	ts, err := OpenTargetStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenTargetStore: %v", err)
	}
	defer ts.Close()

	// Startup calls this to tell a fresh store from a provisioned one, so the
	// empty case has to be distinguishable from "has targets" without relying
	// on a count.
	has, err := ts.HasAnyTarget(ctx)
	if err != nil || has {
		t.Fatalf("HasAnyTarget on empty store = %v, err %v; want false", has, err)
	}

	if err := ts.PutTargetByHost(ctx, "a.example.com", []byte("{}")); err != nil {
		t.Fatalf("PutTargetByHost: %v", err)
	}
	if has, err = ts.HasAnyTarget(ctx); err != nil || !has {
		t.Fatalf("HasAnyTarget after put = %v, err %v; want true", has, err)
	}

	if err := ts.DeleteTargetByHost(ctx, "a.example.com"); err != nil {
		t.Fatalf("DeleteTargetByHost: %v", err)
	}
	if has, err = ts.HasAnyTarget(ctx); err != nil || has {
		t.Fatalf("HasAnyTarget after delete = %v, err %v; want false", has, err)
	}

	// A store that holds only the non-host global key must not count as
	// provisioned: HasAnyTarget scans the h/ prefix specifically.
	if err := ts.PutGlobal(ctx, []byte("{}")); err != nil {
		t.Fatalf("PutGlobal: %v", err)
	}
	if has, err = ts.HasAnyTarget(ctx); err != nil || has {
		t.Fatalf("HasAnyTarget with only the global key = %v, err %v; want false", has, err)
	}
}

func TestTargetStoreHasTargetByHost(t *testing.T) {
	ctx := context.Background()
	ts, err := OpenTargetStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenTargetStore: %v", err)
	}
	defer ts.Close()

	has, err := ts.HasTargetByHost(ctx, "a.example.com")
	if err != nil || has {
		t.Fatalf("HasTargetByHost on missing = %v, err %v; want false", has, err)
	}

	if err := ts.PutTargetByHost(ctx, "a.example.com", []byte("server_name: a")); err != nil {
		t.Fatalf("PutTargetByHost: %v", err)
	}
	has, err = ts.HasTargetByHost(ctx, "a.example.com")
	if err != nil || !has {
		t.Fatalf("HasTargetByHost after put = %v, err %v; want true", has, err)
	}

	// The existence check must agree with the read it stands in for: a router
	// resolves via HasTargetByHost and then materializes via GetTargetByHost,
	// so a disagreement would surface as a 502 on a valid target.
	data, ok, err := ts.GetTargetByHost(ctx, "a.example.com")
	if err != nil || !ok || string(data) != "server_name: a" {
		t.Fatalf("GetTargetByHost = %q ok %v err %v", data, ok, err)
	}
}

func TestTargetStoreHostCountMatchesListHosts(t *testing.T) {
	ctx := context.Background()
	ts, err := OpenTargetStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenTargetStore: %v", err)
	}
	defer ts.Close()

	if err := ts.PutGlobal(ctx, []byte("{}")); err != nil {
		t.Fatalf("PutGlobal: %v", err)
	}
	const n = 25
	for i := 0; i < n; i++ {
		if err := ts.PutTargetByHost(ctx, fmt.Sprintf("host%d.example.com", i), []byte("{}")); err != nil {
			t.Fatalf("PutTargetByHost(%d): %v", i, err)
		}
	}

	count, err := ts.HostCount(ctx)
	if err != nil {
		t.Fatalf("HostCount: %v", err)
	}
	if count != n {
		t.Fatalf("HostCount = %d, want %d (global key must not be counted)", count, n)
	}
}
