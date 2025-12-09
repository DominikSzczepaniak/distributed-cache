package controller

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReaper_Track(t *testing.T) {
	r := NewReaper(5*time.Second, func(nodeID string) {})
	r.Track("node-1")

	r.mu.RLock()
	seen, ok := r.lastSeen["node-1"]
	r.mu.RUnlock()

	assert.True(t, ok)
	assert.WithinDuration(t, time.Now(), seen, 100*time.Millisecond)
}

func TestReaper_CheckLiveness_DeadNode(t *testing.T) {
	var deadNode string
	var wg sync.WaitGroup
	wg.Add(1)

	r := NewReaper(100*time.Millisecond, func(nodeID string) {
		deadNode = nodeID
		wg.Done()
	})

	r.mu.Lock()
	r.lastSeen["node-dead"] = time.Now().Add(-200 * time.Millisecond)
	r.mu.Unlock()

	r.checkLiveness()

	wg.Wait()
	assert.Equal(t, "node-dead", deadNode)

	r.mu.RLock()
	_, ok := r.lastSeen["node-dead"]
	r.mu.RUnlock()
	assert.False(t, ok)
}

func TestReaper_CheckLiveness_LiveNode(t *testing.T) {
	called := false
	r := NewReaper(1*time.Second, func(nodeID string) {
		called = true
	})

	r.Track("node-live")
	r.checkLiveness()

	assert.False(t, called)

	r.mu.RLock()
	_, ok := r.lastSeen["node-live"]
	r.mu.RUnlock()
	assert.True(t, ok)
}
