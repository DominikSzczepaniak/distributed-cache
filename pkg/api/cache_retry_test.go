package api

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClock is a goroutine-safe manual clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestIdempotencyCache_GetSetAndExpiry(t *testing.T) {
	clk := newFakeClock()
	c := newIdempotencyCacheWithClock(time.Minute, clk.Now)

	_, ok := c.Get("missing")
	assert.False(t, ok)

	c.Set("a", raft.BroadcastResponse{Success: true})
	c.Set("b", raft.BroadcastResponse{Success: false})

	resp, ok := c.Get("a")
	require.True(t, ok)
	assert.True(t, resp.Success)
	resp, ok = c.Get("b")
	require.True(t, ok)
	assert.False(t, resp.Success, "entries are independent")

	clk.Advance(time.Minute) // exactly TTL: still valid (strict >)
	_, ok = c.Get("a")
	assert.True(t, ok)

	clk.Advance(time.Second)
	_, ok = c.Get("a")
	assert.False(t, ok, "expired after ttl")
}

func TestIdempotencyCache_SetRefreshesExpiry(t *testing.T) {
	clk := newFakeClock()
	c := newIdempotencyCacheWithClock(time.Minute, clk.Now)
	c.Set("a", raft.BroadcastResponse{Success: true})
	clk.Advance(50 * time.Second)
	c.Set("a", raft.BroadcastResponse{Success: false})
	clk.Advance(50 * time.Second)
	resp, ok := c.Get("a")
	require.True(t, ok)
	assert.False(t, resp.Success, "overwrite wins and resets completion time")
}

func TestIdempotencyCache_BackgroundCleanupRemovesExpired(t *testing.T) {
	clk := newFakeClock()
	c := newIdempotencyCacheWithClock(40*time.Millisecond, clk.Now) // ticker fires every 20ms of real time
	c.Set("a", raft.BroadcastResponse{Success: true})
	clk.Advance(time.Second)

	assert.Eventually(t, func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return len(c.entries) == 0
	}, 2*time.Second, 10*time.Millisecond, "cleanup goroutine must evict expired entries")
}

func TestIdempotencyCache_ConcurrentAccess(t *testing.T) {
	c := NewIdempotencyCache(time.Minute)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				k := fmt.Sprintf("k%d", j%10)
				c.Set(k, raft.BroadcastResponse{Success: true})
				c.Get(k)
			}
		}(i)
	}
	wg.Wait()
}

func TestLeaderCache_SetGetExpiryInvalidate(t *testing.T) {
	clk := newFakeClock()
	lc := NewLeaderCache(time.Second)
	lc.now = clk.Now

	_, _, ok := lc.Get()
	assert.False(t, ok, "empty cache is a miss")

	lc.Set(2, "10.0.0.2:8080")
	id, addr, ok := lc.Get()
	require.True(t, ok)
	assert.Equal(t, 2, id)
	assert.Equal(t, "10.0.0.2:8080", addr)

	clk.Advance(time.Second)
	_, _, ok = lc.Get()
	assert.True(t, ok, "exactly ttl still valid")

	clk.Advance(time.Millisecond)
	id, addr, ok = lc.Get()
	assert.False(t, ok)
	assert.Equal(t, -1, id)
	assert.Empty(t, addr)

	lc.Set(3, "x")
	lc.Invalidate()
	_, _, ok = lc.Get()
	assert.False(t, ok, "Invalidate forces a miss")

	lc.Set(4, "y")
	id, _, ok = lc.Get()
	assert.True(t, ok)
	assert.Equal(t, 4, id, "leader change overwrites")
}

func fastRetry(attempts int) *Retrier {
	return NewRetrier(RetryConfig{MaxAttempts: attempts, InitialDelay: time.Millisecond, MaxDelay: 4 * time.Millisecond, Multiplier: 2})
}

func TestRetrier_SuccessFirstTry(t *testing.T) {
	var n int
	err := fastRetry(3).ExecuteWithRetry(context.Background(), func(context.Context) error { n++; return nil })
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestRetrier_HonorsMaxAttempts(t *testing.T) {
	var n int
	sentinel := errors.New("connection refused")
	err := fastRetry(4).ExecuteWithRetry(context.Background(), func(context.Context) error { n++; return sentinel })
	require.Error(t, err)
	assert.Equal(t, 4, n)
	assert.ErrorIs(t, err, sentinel, "last error must be wrapped")
	assert.Contains(t, err.Error(), "max retry attempts (4) exceeded")
}

func TestRetrier_NonRetryableStopsImmediately(t *testing.T) {
	for _, msg := range []string{"invalid key", "bad request", "key not found", "conflict detected"} {
		var n int
		err := fastRetry(5).ExecuteWithRetry(context.Background(), func(context.Context) error { n++; return errors.New(msg) })
		require.Error(t, err)
		assert.Equal(t, 1, n, msg)
	}
}

func TestRetrier_RetryableBeatsNonRetryable(t *testing.T) {
	// "timeout" is checked first, so a message with both is retried.
	var n int
	_ = fastRetry(3).ExecuteWithRetry(context.Background(), func(context.Context) error { n++; return errors.New("timeout: invalid") })
	assert.Equal(t, 3, n)
}

func TestRetrier_StopsOnContextCancelDuringBackoff(t *testing.T) {
	r := NewRetrier(RetryConfig{MaxAttempts: 10, InitialDelay: time.Hour, MaxDelay: time.Hour, Multiplier: 1})
	ctx, cancel := context.WithCancel(context.Background())
	var n atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- r.ExecuteWithRetry(ctx, func(context.Context) error { n.Add(1); return errors.New("timeout") })
	}()
	require.Eventually(t, func() bool { return n.Load() == 1 }, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("retrier did not stop on ctx cancel (goroutine leak)")
	}
	assert.EqualValues(t, 1, n.Load(), "no further attempts after cancel")
}

func TestRetrier_AlreadyCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var n int
	err := fastRetry(5).ExecuteWithRetry(ctx, func(ctx context.Context) error { n++; return ctx.Err() })
	require.Error(t, err)
	assert.LessOrEqual(t, n, 1, "must not spin attempts on a dead context")
}

func TestRetrier_BackoffGrowthCapAndJitter(t *testing.T) {
	r := NewRetrier(RetryConfig{InitialDelay: 100 * time.Millisecond, MaxDelay: 500 * time.Millisecond, Multiplier: 2})
	assert.Equal(t, 100*time.Millisecond, r.calculateBackoff(0))
	assert.Equal(t, 200*time.Millisecond, r.calculateBackoff(1))
	assert.Equal(t, 400*time.Millisecond, r.calculateBackoff(2))
	assert.Equal(t, 500*time.Millisecond, r.calculateBackoff(3), "capped at MaxDelay")
	assert.Equal(t, 500*time.Millisecond, r.calculateBackoff(20))

	rj := NewRetrier(RetryConfig{InitialDelay: 100 * time.Millisecond, MaxDelay: time.Second, Multiplier: 2, Jitter: true})
	for i := 0; i < 200; i++ {
		d := rj.calculateBackoff(1) // base 200ms, jitter within +-15%
		assert.GreaterOrEqual(t, d, 170*time.Millisecond)
		assert.LessOrEqual(t, d, 230*time.Millisecond)
	}
}

func TestRetrier_DefaultConfigsSane(t *testing.T) {
	for _, op := range []string{"PUT", "DELETE", "GET"} {
		cfg, ok := DefaultRetryConfigs[op]
		require.True(t, ok, op)
		assert.Greater(t, cfg.MaxAttempts, 1)
		assert.LessOrEqual(t, cfg.InitialDelay, cfg.MaxDelay)
	}
	assert.False(t, DefaultRetryConfigs["GET"].EnableIdempotency)
}
