package cdnscale

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