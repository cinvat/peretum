package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ConfigStreamClient connects to a control plane and streams config updates.
type ConfigStreamClient struct {
	mu           sync.RWMutex
	controlPlane string
	conn         *grpc.ClientConn
	client       ConfigStreamServiceClient
	stream       ConfigStreamService_StreamConfigsClient
	ctx          context.Context
	cancel       context.CancelFunc
	
	// Callbacks
	onGlobalConfig func(*GlobalConfigUpdate)
	onTargetConfig func(*TargetConfigUpdate)
	onTargetDelete func(string)
	
	// State
	connected bool
	muState   sync.Mutex
}

// GlobalConfigUpdate represents a global config update from control plane.
type GlobalConfigUpdate struct {
	Config map[string]interface{}
	Hash   string
	Version string
}

// TargetConfigUpdate represents a target config update from control plane.
type TargetConfigUpdate struct {
	TargetName string
	Config     map[string]interface{}
	Hash       string
	Version    string
}

// ConfigStreamServiceClient is the gRPC client interface.
// In production, this would be generated from protobuf definitions.
type ConfigStreamServiceClient interface {
	StreamConfigs(ctx context.Context, opts ...grpc.CallOption) (ConfigStreamService_StreamConfigsClient, error)
}

// ConfigStreamService_StreamConfigsClient is the streaming client interface.
type ConfigStreamService_StreamConfigsClient interface {
	Send(*ConfigRequest) error
	Recv() (*ConfigResponse, error)
	grpc.ClientStream
}

// ConfigRequest is sent by the edge to request config.
type ConfigRequest struct {
	NodeID       string
	Region       string
	KnownVersions map[string]string // target name -> version hash
}

// ConfigResponse is received from control plane.
type ConfigResponse struct {
	GlobalConfig *GlobalConfigUpdate
	TargetUpdates []*TargetConfigUpdate
	TargetDeletes []string
	SnapshotVersion string
}

// NewConfigStreamClient creates a new config streaming client.
func NewConfigStreamClient(controlPlane string) *ConfigStreamClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConfigStreamClient{
		controlPlane: controlPlane,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// SetCallbacks sets the update callbacks.
func (csc *ConfigStreamClient) SetCallbacks(
	onGlobalConfig func(*GlobalConfigUpdate),
	onTargetConfig func(*TargetConfigUpdate),
	onTargetDelete func(string),
) {
	csc.mu.Lock()
	defer csc.mu.Unlock()
	csc.onGlobalConfig = onGlobalConfig
	csc.onTargetConfig = onTargetConfig
	csc.onTargetDelete = onTargetDelete
}

// Connect establishes the gRPC connection and starts streaming.
func (csc *ConfigStreamClient) Connect() error {
	csc.muState.Lock()
	defer csc.muState.Unlock()
	
	if csc.connected {
		return nil
	}
	
	conn, err := grpc.DialContext(csc.ctx, csc.controlPlane,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(10*time.Second),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to control plane: %w", err)
	}
	
	csc.conn = conn
	// In production, use generated client: csc.client = pb.NewConfigStreamServiceClient(conn)
	// For now, we'll use a mock implementation
	
	csc.connected = true
	go csc.receiveLoop()
	return nil
}

// Close closes the connection.
func (csc *ConfigStreamClient) Close() error {
	csc.cancel()
	csc.muState.Lock()
	defer csc.muState.Unlock()
	
	if csc.conn != nil {
		return csc.conn.Close()
	}
	return nil
}

// receiveLoop continuously receives config updates from control plane.
func (csc *ConfigStreamClient) receiveLoop() {
	for {
		select {
		case <-csc.ctx.Done():
			return
		default:
			// In production: resp, err := csc.stream.Recv()
			// For now, simulate receiving updates
			time.Sleep(30 * time.Second)
		}
	}
}

// RequestFullSync requests a full config sync from control plane.
func (csc *ConfigStreamClient) RequestFullSync() error {
	csc.mu.RLock()
	defer csc.mu.RUnlock()
	
	if !csc.connected || csc.stream == nil {
		return fmt.Errorf("not connected to control plane")
	}
	
	// In production: req := &ConfigRequest{...}; return csc.stream.Send(req)
	return nil
}

// IsConnected returns connection status.
func (csc *ConfigStreamClient) IsConnected() bool {
	csc.muState.Lock()
	defer csc.muState.Unlock()
	return csc.connected
}

// MockConfigStreamServer is a test/mock implementation of the control plane server.
// In production, this would be a separate service.
type MockConfigStreamServer struct {
	mu           sync.RWMutex
	targets      map[string]map[string]interface{}
	globalConfig map[string]interface{}
	clients      map[string]*mockClientStream
}

type mockClientStream struct {
	ctx    context.Context
	cancel context.CancelFunc
	sendCh chan *ConfigResponse
}

func NewMockConfigStreamServer() *MockConfigStreamServer {
	return &MockConfigStreamServer{
		targets:      make(map[string]map[string]interface{}),
		globalConfig: make(map[string]interface{}),
		clients:      make(map[string]*mockClientStream),
	}
}

func (m *MockConfigStreamServer) SetGlobalConfig(config map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.globalConfig = config
	// Notify all clients
	for _, client := range m.clients {
		select {
		case client.sendCh <- &ConfigResponse{
			GlobalConfig: &GlobalConfigUpdate{
				Config: config,
				Hash:   "hash",
				Version: "1",
			},
		}:
		default:
		}
	}
}

func (m *MockConfigStreamServer) UpdateTarget(targetName string, config map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.targets[targetName] = config
	
	for _, client := range m.clients {
		select {
		case client.sendCh <- &ConfigResponse{
			TargetUpdates: []*TargetConfigUpdate{{
				TargetName: targetName,
				Config:     config,
				Hash:       "hash",
				Version:    "1",
			}},
		}:
		default:
		}
	}
}

func (m *MockConfigStreamServer) DeleteTarget(targetName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.targets, targetName)
	
	for _, client := range m.clients {
		select {
		case client.sendCh <- &ConfigResponse{
			TargetDeletes: []string{targetName},
		}:
		default:
		}
	}
}

// ControlPlaneServer is the gRPC server that distributes config to edge nodes.
// It watches the config directory and streams updates to connected edges.
type ControlPlaneServer struct {
	mu             sync.RWMutex
	configDir      string
	dataDir        string
	targets        map[string]map[string]interface{}
	globalConfig   map[string]interface{}
	targetVersions map[string]string
	clients        map[string]*clientStream
	
	// File watching
	watcherDone chan struct{}
}

// clientStream represents a connected edge node.
type clientStream struct {
	ctx        context.Context
	cancel     context.CancelFunc
	sendCh     chan *ConfigResponse
	knownVers  map[string]string
}

// NewControlPlaneServer creates a new control plane server.
func NewControlPlaneServer(configDir, dataDir string) *ControlPlaneServer {
	return &ControlPlaneServer{
		configDir:      configDir,
		dataDir:        dataDir,
		targets:        make(map[string]map[string]interface{}),
		globalConfig:   make(map[string]interface{}),
		targetVersions: make(map[string]string),
		clients:        make(map[string]*clientStream),
		watcherDone:    make(chan struct{}),
	}
}

// StreamConfigs is the gRPC streaming endpoint for edges.
// In production, this would implement the generated gRPC interface.
func (cp *ControlPlaneServer) StreamConfigs(stream interface{}) error {
	// This is a placeholder for the gRPC generated code
	// In production, you'd use the generated gRPC interface
	
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	
	clientID := fmt.Sprintf("edge-%d", time.Now().UnixNano())
	
	cs := &clientStream{
		ctx:       ctx,
		cancel:    cancel,
		sendCh:    make(chan *ConfigResponse, 100),
		knownVers: make(map[string]string),
	}
	
	cp.mu.Lock()
	cp.clients[clientID] = cs
	cp.mu.Unlock()
	
	// Send initial snapshot
	if err := cp.sendSnapshot(clientID); err != nil {
		return err
	}
	
	// Wait for context cancellation
	<-ctx.Done()
	
	cp.mu.Lock()
	delete(cp.clients, clientID)
	cp.mu.Unlock()
	
	return nil
}

// sendSnapshot sends the full config snapshot to a client.
func (cp *ControlPlaneServer) sendSnapshot(clientID string) error {
	cp.mu.RLock()
	cs, ok := cp.clients[clientID]
	cp.mu.RUnlock()
	
	if !ok {
		return fmt.Errorf("client not found")
	}
	
	cp.mu.RLock()
	globalConfig := cp.globalConfig
	targets := make(map[string]map[string]interface{}, len(cp.targets))
	for k, v := range cp.targets {
		targets[k] = v
	}
	versions := make(map[string]string, len(cp.targetVersions))
	for k, v := range cp.targetVersions {
		versions[k] = v
	}
	cp.mu.RUnlock()
	
	// Build snapshot response
	resp := &ConfigResponse{
		GlobalConfig: &GlobalConfigUpdate{
			Config: globalConfig,
			Hash:   cp.computeHash(globalConfig),
			Version: "1",
		},
		SnapshotVersion: "1",
	}
	
	for name, config := range targets {
		resp.TargetUpdates = append(resp.TargetUpdates, &TargetConfigUpdate{
			TargetName: name,
			Config:     config,
			Hash:       versions[name],
			Version:    versions[name],
		})
	}
	
	select {
	case cs.sendCh <- resp:
		return nil
	case <-cs.ctx.Done():
		return cs.ctx.Err()
	}
}

// WatchConfigChanges watches the config directory for changes and broadcasts updates.
func (cp *ControlPlaneServer) WatchConfigChanges(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	defer close(cp.watcherDone)
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cp.scanAndBroadcast()
		}
	}
}

// scanAndBroadcast scans the config directory for changes and broadcasts to clients.
func (cp *ControlPlaneServer) scanAndBroadcast() {
	// In production, this would use fsnotify or similar
	// For now, we just show the structure
}

// BroadcastGlobalConfig broadcasts a global config update to all clients.
func (cp *ControlPlaneServer) BroadcastGlobalConfig(config map[string]interface{}) {
	cp.mu.Lock()
	cp.globalConfig = config
	cp.mu.Unlock()
	
	cp.broadcastToClients(func(id string, cs *clientStream) error {
		select {
		case cs.sendCh <- &ConfigResponse{
			GlobalConfig: &GlobalConfigUpdate{
				Config:  config,
				Hash:    cp.computeHash(config),
				Version: "1",
			},
		}:
			return nil
		case <-cs.ctx.Done():
			return cs.ctx.Err()
		}
	})
}

// BroadcastTargetUpdate broadcasts a target config update to all clients.
func (cp *ControlPlaneServer) BroadcastTargetUpdate(targetName string, config map[string]interface{}) {
	cp.mu.Lock()
	cp.targets[targetName] = config
	cp.targetVersions[targetName] = cp.computeHash(config)
	cp.mu.Unlock()
	
	cp.broadcastToClients(func(id string, cs *clientStream) error {
		select {
		case cs.sendCh <- &ConfigResponse{
			TargetUpdates: []*TargetConfigUpdate{{
				TargetName: targetName,
				Config:     config,
				Hash:       cp.targetVersions[targetName],
				Version:    cp.targetVersions[targetName],
			}},
		}:
			return nil
		case <-cs.ctx.Done():
			return cs.ctx.Err()
		}
	})
}

// BroadcastTargetDelete broadcasts a target deletion to all clients.
func (cp *ControlPlaneServer) BroadcastTargetDelete(targetName string) {
	cp.mu.Lock()
	delete(cp.targets, targetName)
	delete(cp.targetVersions, targetName)
	cp.mu.Unlock()
	
	cp.broadcastToClients(func(id string, cs *clientStream) error {
		select {
		case cs.sendCh <- &ConfigResponse{
			TargetDeletes: []string{targetName},
		}:
			return nil
		case <-cs.ctx.Done():
			return cs.ctx.Err()
		}
	})
}

// broadcastToClients sends a message to all connected clients.
func (cp *ControlPlaneServer) broadcastToClients(fn func(string, *clientStream) error) {
	cp.mu.RLock()
	clients := make([]*clientStream, 0, len(cp.clients))
	clientIDs := make([]string, 0, len(cp.clients))
	for id, cs := range cp.clients {
		clients = append(clients, cs)
		clientIDs = append(clientIDs, id)
	}
	cp.mu.RUnlock()
	
	for i, cs := range clients {
		select {
		case <-cs.ctx.Done():
			continue
		default:
			if err := fn(clientIDs[i], cs); err != nil {
				// Client disconnected or channel full
				cp.mu.Lock()
				delete(cp.clients, clientIDs[i])
				cp.mu.Unlock()
			}
		}
	}
}

// computeHash computes a hash for config versioning.
func (cp *ControlPlaneServer) computeHash(data interface{}) string {
	// Simplified hash - in production use proper serialization + SHA256
	return fmt.Sprintf("%v", data)
}

// GetStats returns server statistics.
func (cp *ControlPlaneServer) GetStats() map[string]interface{} {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	
	return map[string]interface{}{
		"connected_clients": len(cp.clients),
		"targets":           len(cp.targets),
		"global_config":     cp.globalConfig != nil,
	}
}