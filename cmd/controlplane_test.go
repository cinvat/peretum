package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

func TestRunControlPlaneServesSnapshots(t *testing.T) {
	// Start embedded NATS server with JetStream
	ns, natsErr := server.NewServer(&server.Options{
		Port:      -1, // random port
		JetStream: true,
	})
	if natsErr != nil {
		t.Fatal(natsErr)
	}
	ns.Start()
	defer ns.Shutdown()

	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready")
	}

	natsURL := ns.ClientURL()

	dir := t.TempDir()
	cfgDir := filepath.Join(dir, "config.d")
	if err := os.MkdirAll(cfgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(cfgDir, "api.yaml"), `server_name: api
upstreams:
  - url: http://api:1
locations:
  - path: /
`)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	httpAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	go func() {
		done <- runControlPlane(ctx, natsURL, httpAddr, cfgDir, filepath.Join(dir, "data"))
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("control plane did not shut down")
		}
	})

	base := "http://" + httpAddr
	var resp *http.Response
	resp = waitForServer(t, base+"/health")
	resp.Body.Close()

	// Full snapshot
	var err error
	resp, err = http.Get(base + "/sync")
	if err != nil {
		t.Fatalf("GET /sync: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /sync status = %d", resp.StatusCode)
	}
	var snap struct {
		Targets map[string]map[string]interface{} `json:"targets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode /sync: %v", err)
	}
	if len(snap.Targets) != 1 {
		t.Fatalf("snapshot targets = %v", snap.Targets)
	}
	entry, ok := snap.Targets["api"]
	if !ok {
		t.Fatalf("missing api target in snapshot: %v", snap.Targets)
	}
	if entry["version"] == "" || entry["version"] == nil {
		t.Fatalf("snapshot entry missing version: %v", entry)
	}

	// Single target snapshot
	resp, err = http.Post(base+"/sync/target", "application/json",
		strings.NewReader(`{"server_name":"api"}`))
	if err != nil {
		t.Fatalf("POST /sync/target: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("POST /sync/target status = %d body=%s", resp.StatusCode, body)
	}
	resp.Body.Close()

	resp, err = http.Post(base+"/sync/target", "application/json",
		strings.NewReader(`{"server_name":"missing"}`))
	if err != nil {
		t.Fatalf("POST /sync/target (missing): %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("POST /sync/target missing status = %d body=%s", resp.StatusCode, body)
	}
	resp.Body.Close()
}
