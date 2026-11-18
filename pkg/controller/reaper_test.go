package controller

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testClock is a goroutine-safe manual clock.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func newTestClock() *testClock { return &testClock{t: time.Unix(1_700_000_000, 0)} }
func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestReaper_Track(t *testing.T) {
	clk := newTestClock()
	deaths := make(chan string, 10)
	r := newReaperWithClock(5*time.Second, func(id string) { deaths <- id }, clk.Now)

	r.Track("node-1")
	r.Track("node-2")

	// node-1 keeps heartbeating, node-2 goes silent.
	for i := 0; i < 4; i++ {
		clk.Advance(2 * time.Second)
		r.Track("node-1")
		r.checkLiveness()
	}

	select {
	case id := <-deaths:
		assert.Equal(t, "node-2", id, "only the silent node is declared dead")
	case <-time.After(time.Second):
		t.Fatal("silent node was not declared dead after grace period")
	}

	clk.Advance(2 * time.Second)
	r.Track("node-1")
	r.checkLiveness()
	select {
	case id := <-deaths:
		t.Fatalf("unexpected death report for %s (heartbeating node or duplicate report)", id)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestReaper_Track_Boundary(t *testing.T) {
	clk := newTestClock()
	deaths := make(chan string, 1)
	r := newReaperWithClock(5*time.Second, func(id string) { deaths <- id }, clk.Now)
	r.Track("n")

	clk.Advance(5 * time.Second) // exactly grace: still alive (strict >)
	r.checkLiveness()
	select {
	case <-deaths:
		t.Fatal("node declared dead at exactly the grace period")
	case <-time.After(30 * time.Millisecond):
	}

	clk.Advance(time.Nanosecond)
	r.checkLiveness()
	select {
	case id := <-deaths:
		assert.Equal(t, "n", id)
	case <-time.After(time.Second):
		t.Fatal("node not declared dead after grace period elapsed")
	}
}

func TestReaper_Track_RevivedNodeIsTrackedAgain(t *testing.T) {
	clk := newTestClock()
	deaths := make(chan string, 4)
	r := newReaperWithClock(time.Second, func(id string) { deaths <- id }, clk.Now)
	r.Track("n")
	clk.Advance(2 * time.Second)
	r.checkLiveness()
	require.Equal(t, "n", <-deaths)

	r.Track("n") // node restarts and heartbeats again
	clk.Advance(2 * time.Second)
	r.checkLiveness()
	select {
	case id := <-deaths:
		assert.Equal(t, "n", id, "a revived-then-silent node is reaped again")
	case <-time.After(time.Second):
		t.Fatal("revived node never reaped again")
	}
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
