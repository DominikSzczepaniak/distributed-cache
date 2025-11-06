package raft

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

type ConnectionManager struct {
	mu sync.RWMutex

	conns         []*grpc.ClientConn
	peers         []PeerClient
	peerAvailable []atomic.Bool
	lastContact   []time.Time

	selfID      int
	totalNodes  int
	addrs       []string
	retryCfg    RetryConfig
	connTimeout time.Duration
	healthInterval time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewConnectionManager(selfID, totalNodes int, addrs []string, cfg *Config) *ConnectionManager {
	ctx, cancel := context.WithCancel(context.Background())

	cm := &ConnectionManager{
		conns:          make([]*grpc.ClientConn, totalNodes),
		peers:          make([]PeerClient, totalNodes),
		peerAvailable:  make([]atomic.Bool, totalNodes),
		lastContact:    make([]time.Time, totalNodes),
		selfID:         selfID,
		totalNodes:     totalNodes,
		addrs:          addrs,
		retryCfg:       cfg.connectionRetryConfig,
		connTimeout:    cfg.connectionTimeout,
		healthInterval: cfg.healthCheckInterval,
		ctx:            ctx,
		cancel:         cancel,
	}

	for i := 0; i < totalNodes; i++ {
		if i == selfID {
			cm.peerAvailable[i].Store(true)
			continue
		}
		cm.wg.Add(1)
		go cm.connectPeerAsync(i)
	}

	cm.wg.Add(1)
	go cm.healthCheckLoop()

	return cm
}

func (cm *ConnectionManager) connectPeerAsync(peerID int) {
	defer cm.wg.Done()

	backoff := cm.retryCfg.InitialBackoff
	attempts := 0

	for {
		select {
		case <-cm.ctx.Done():
			return
		default:
		}

		if cm.retryCfg.MaxRetries > 0 && attempts >= cm.retryCfg.MaxRetries {
			slog.Error(fmt.Sprintf("Node %d: Failed to connect to peer %d after %d attempts",
				cm.selfID, peerID, attempts))
			return
		}

		conn, peer, err := cm.dialPeer(peerID)
		if err != nil {
			slog.Warn(fmt.Sprintf("Node %d: Connection attempt %d to peer %d failed: %v. Retrying in %v",
				cm.selfID, attempts+1, peerID, err, backoff))

			select {
			case <-time.After(backoff):
				backoff = time.Duration(float64(backoff) * cm.retryCfg.Multiplier)
				if backoff > cm.retryCfg.MaxBackoff {
					backoff = cm.retryCfg.MaxBackoff
				}
				attempts++
				continue
			case <-cm.ctx.Done():
				return
			}
		}

		cm.mu.Lock()
		cm.conns[peerID] = conn
		cm.peers[peerID] = peer
		cm.lastContact[peerID] = time.Now()
		cm.mu.Unlock()

		cm.peerAvailable[peerID].Store(true)
		slog.Info(fmt.Sprintf("Node %d: Successfully connected to peer %d at %s after %d attempts",
			cm.selfID, peerID, cm.addrs[peerID], attempts+1))
		return
	}
}

func (cm *ConnectionManager) dialPeer(peerID int) (*grpc.ClientConn, PeerClient, error) {
	addr := cm.addrs[peerID]

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	ctx, cancel := context.WithTimeout(cm.ctx, cm.connTimeout)
	defer cancel()

	for {
		state := conn.GetState()
		if state == connectivity.Ready || state == connectivity.Idle {
			peer := NewGRPCPeerClient(conn)
			return conn, peer, nil
		}

		if state == connectivity.TransientFailure || state == connectivity.Shutdown {
			conn.Close()
			return nil, nil, fmt.Errorf("connection to %s failed: state=%v", addr, state)
		}

		if !conn.WaitForStateChange(ctx, state) {
			conn.Close()
			return nil, nil, fmt.Errorf("timeout waiting for connection to %s", addr)
		}
	}
}

func (cm *ConnectionManager) healthCheckLoop() {
	defer cm.wg.Done()

	if cm.healthInterval <= 0 {
		slog.Debug(fmt.Sprintf("Node %d: Health check disabled (interval: %v)", cm.selfID, cm.healthInterval))
		return
	}

	ticker := time.NewTicker(cm.healthInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cm.ctx.Done():
			return
		case <-ticker.C:
			cm.performHealthCheck()
		}
	}
}

func (cm *ConnectionManager) performHealthCheck() {
	for i := 0; i < cm.totalNodes; i++ {
		if i == cm.selfID {
			continue
		}

		cm.mu.RLock()
		conn := cm.conns[i]
		cm.mu.RUnlock()

		if conn == nil {
			if cm.peerAvailable[i].Load() {
				slog.Debug(fmt.Sprintf("Node %d: Peer %d connection is nil but marked available",
					cm.selfID, i))
				cm.peerAvailable[i].Store(false)
			}
			continue
		}

		state := conn.GetState()

		switch state {
		case connectivity.Ready, connectivity.Idle:
			if !cm.peerAvailable[i].Load() {
				slog.Info(fmt.Sprintf("Node %d: Peer %d is now available (state: %v)",
					cm.selfID, i, state))
				cm.peerAvailable[i].Store(true)
			}
			cm.mu.Lock()
			cm.lastContact[i] = time.Now()
			cm.mu.Unlock()

		case connectivity.TransientFailure, connectivity.Shutdown:
			if cm.peerAvailable[i].Load() {
				slog.Warn(fmt.Sprintf("Node %d: Peer %d became unavailable (state: %v)",
					cm.selfID, i, state))
				cm.peerAvailable[i].Store(false)
			}

			if state == connectivity.Shutdown {
				cm.reconnectPeer(i)
			}

		case connectivity.Connecting:
			// Wait for connection attempt to complete
		}
	}
}

func (cm *ConnectionManager) reconnectPeer(peerID int) {
	cm.mu.Lock()
	oldConn := cm.conns[peerID]
	cm.conns[peerID] = nil
	cm.peers[peerID] = nil
	cm.mu.Unlock()

	if oldConn != nil {
		oldConn.Close()
	}

	slog.Info(fmt.Sprintf("Node %d: Initiating reconnection to peer %d", cm.selfID, peerID))
	cm.peerAvailable[peerID].Store(false)

	cm.wg.Add(1)
	go cm.connectPeerAsync(peerID)
}

func (cm *ConnectionManager) GetPeer(peerID int) PeerClient {
	if peerID < 0 || peerID >= cm.totalNodes {
		return nil
	}

	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.peers[peerID]
}

func (cm *ConnectionManager) GetPeers() []PeerClient {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	result := make([]PeerClient, cm.totalNodes)
	copy(result, cm.peers)
	return result
}

func (cm *ConnectionManager) IsPeerAvailable(peerID int) bool {
	if peerID < 0 || peerID >= cm.totalNodes {
		return false
	}
	return cm.peerAvailable[peerID].Load()
}

func (cm *ConnectionManager) GetAvailablePeerCount() int {
	count := 0
	for i := 0; i < cm.totalNodes; i++ {
		if i == cm.selfID {
			continue
		}
		if cm.peerAvailable[i].Load() {
			count++
		}
	}
	return count
}

func (cm *ConnectionManager) Close() {
	slog.Info(fmt.Sprintf("Node %d: Closing connection manager", cm.selfID))
	cm.cancel()
	cm.wg.Wait()

	cm.mu.Lock()
	defer cm.mu.Unlock()

	for i, conn := range cm.conns {
		if conn != nil && i != cm.selfID {
			conn.Close()
		}
	}
}
