package cluster

import (
	"context"
	"net/http"
	"sync"
	"time"
)

type LeadershipState int

const (
	StateFollower LeadershipState = iota
	StateCandidate
	StateLeader
)

type LeaderElection struct {
	mu           sync.RWMutex
	state        LeadershipState
	nodeID       string
	peerURLs     []string
	healthClient *http.Client

	onBecomeLeader   func()
	onLoseLeadership func()

	checkInterval time.Duration
	stopCh        chan struct{}
}

func NewLeaderElection(nodeID string, peerURLs []string, checkInterval time.Duration) *LeaderElection {
	if checkInterval == 0 {
		checkInterval = 2 * time.Second
	}
	return &LeaderElection{
		nodeID:        nodeID,
		peerURLs:      peerURLs,
		checkInterval: checkInterval,
		healthClient: &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        10,
				MaxIdleConnsPerHost: 5,
				IdleConnTimeout:     30 * time.Second,
			},
		},
		stopCh: make(chan struct{}),
		state:  StateFollower,
	}
}

func (le *LeaderElection) Start(ctx context.Context, onLeader, onFollower func()) {
	le.onBecomeLeader = onLeader
	le.onLoseLeadership = onFollower

	go le.runElectionLoop(ctx)
}

func (le *LeaderElection) runElectionLoop(ctx context.Context) {
	ticker := time.NewTicker(le.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-le.stopCh:
			return
		case <-ticker.C:
			le.checkLeadership(ctx)
		}
	}
}

func (le *LeaderElection) checkLeadership(ctx context.Context) {
	le.mu.Lock()
	defer le.mu.Unlock()

	switch le.state {
	case StateFollower:
		if !le.isLeaderAlive(ctx) {
			le.becomeCandidate(ctx)
		}
	case StateCandidate:
		if le.tryBecomeLeader(ctx) {
			le.becomeLeader()
		}
	case StateLeader:
		if !le.isSelfAlive(ctx) {
			le.becomeFollower()
		}
	}
}

func (le *LeaderElection) becomeCandidate(ctx context.Context) {
	le.state = StateCandidate
	// Try to become leader immediately
	if le.tryBecomeLeader(ctx) {
		le.becomeLeader()
	}
}

func (le *LeaderElection) isLeaderAlive(ctx context.Context) bool {
	for _, url := range le.peerURLs {
		req, _ := http.NewRequestWithContext(ctx, "GET", url+"/health/leader", nil)
		resp, err := le.healthClient.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return true
		}
		if resp != nil {
			resp.Body.Close()
		}
	}
	return false
}

func (le *LeaderElection) tryBecomeLeader(ctx context.Context) bool {
	for _, url := range le.peerURLs {
		req, _ := http.NewRequestWithContext(ctx, "POST", url+"/leadership/claim", nil)
		req.Header.Set("X-Candidate-ID", le.nodeID)

		resp, err := le.healthClient.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return true
		}
		if resp != nil {
			resp.Body.Close()
		}
	}
	return false
}

func (le *LeaderElection) becomeLeader() {
	le.state = StateLeader
	if le.onBecomeLeader != nil {
		go le.onBecomeLeader()
	}
}

func (le *LeaderElection) becomeFollower() {
	le.state = StateFollower
	if le.onLoseLeadership != nil {
		go le.onLoseLeadership()
	}
}

func (le *LeaderElection) isSelfAlive(ctx context.Context) bool {
	return true
}

func (le *LeaderElection) IsLeader() bool {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.state == StateLeader
}

func (le *LeaderElection) Stop() {
	close(le.stopCh)
}

func (le *LeaderElection) State() LeadershipState {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.state
}
