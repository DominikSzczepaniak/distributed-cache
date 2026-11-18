package api

// Tests derived from the code review of split/03-api-idempotency.
//
// Tests asserting the CORRECT behaviour of a problem still present in the
// production code are named TestKnownBug_* (they fail today on purpose and are
// excluded from the required CI gate with `-skip TestKnownBug`).
//
// Not covered by a Go test, on purpose:
//   - "pkg/api is not wired into any binary": that is a product decision (wire
//     it or delete it), not a behaviour.
//   - "generateIdempotencyToken / leaderCache are dead code": caught by a
//     linter (staticcheck U1000), not by a test.
//   - "e2e through a real cluster with a leader kill during a write": needs
//     pkg/api to be served by a binary first.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func deleteHandler(s *Server, key int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { s.handleDelete(w, r, key) }
}

// ---------------------------------------------------------------------------
// High 1: the server's own retry re-proposes after a commit-ambiguous timeout
// ---------------------------------------------------------------------------

func TestKnownBug_InternalRetryAfterAmbiguousTimeoutProposesOnce(t *testing.T) {
	var mu sync.Mutex
	state := map[int32]int32{7: 1}
	var calls atomic.Int32
	b := &fakeBackend{forwardFn: func(_ context.Context, m *raftpb.Message) (*raftpb.ForwardResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		delete(state, m.Key) // the entry commits ...
		if calls.Add(1) == 1 {
			state[7] = 9 // ... another client writes k=9 ...
			return nil, errors.New("timeout waiting for response after 5s")
		}
		return &raftpb.ForwardResponse{Success: true}, nil
	}}
	s := newTestServer(b)

	rec := do(t, deleteHandler(s, 7), http.MethodDelete, "/kv/7", "", map[string]string{"Idempotency-Key": "del-1"})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.EqualValues(t, 1, calls.Load(), "one logical DELETE was proposed %d times", calls.Load())
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, int32(9), state[7], "the other client's later write was destroyed by the retry")
}

// The root fix is deduplication in the replicated state machine. The only
// production application today ignores the token, so a re-proposed command is
// applied twice and an old topology overwrites a newer one.
// (The matching test for ControllerApp lives in pkg/controller.)
func TestRetryReusesTheSameIdempotencyTokenAndClient(t *testing.T) {
	var calls atomic.Int32
	b := &fakeBackend{forwardFn: func(context.Context, *raftpb.Message) (*raftpb.ForwardResponse, error) {
		if calls.Add(1) < 3 {
			return nil, errors.New("timeout waiting for response")
		}
		return &raftpb.ForwardResponse{Success: true}, nil
	}}
	s := newTestServer(b)
	rec := do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":1}`,
		map[string]string{"Idempotency-Key": "k", "X-Client-ID": "c1"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.Len(t, b.forwarded, 3)
	for i, m := range b.forwarded {
		assert.Equal(t, "k", m.IdempotencyToken, "attempt %d", i)
		assert.Equal(t, "c1", m.ClientId, "attempt %d", i)
	}
}

// ---------------------------------------------------------------------------
// High 2: same key, different request
// ---------------------------------------------------------------------------

func TestKnownBug_SameKeyDifferentBodyIsRejected(t *testing.T) {
	b := &fakeBackend{}
	s := newTestServer(b)
	hdr := map[string]string{"Idempotency-Key": "k", "X-Client-ID": "A"}

	do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":10}`, hdr)
	rec := do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":20}`, hdr)

	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code,
		"a different payload under a used key must be rejected (422), got %d X-Cache=%q", rec.Code, rec.Header().Get("X-Cache"))
	assert.EqualValues(t, 1, b.forwardCnt.Load())
}

// Documents that the existing test
// TestHandlePut_PutAndDeleteTokensShareNamespaceOnlyWhenEqual asserts the bug
// as "documented behaviour": a DELETE must never be answered with the PUT's
// cached response without being executed.
func TestKnownBug_DeleteReusingAPutKeyIsNotReplayedAsSuccess(t *testing.T) {
	b := &fakeBackend{}
	s := newTestServer(b)
	hdr := map[string]string{"Idempotency-Key": "same", "X-Client-ID": "A"}

	do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":1}`, hdr)
	rec := do(t, deleteHandler(s, 1), http.MethodDelete, "/kv/1", "", hdr)

	replayed := rec.Header().Get("X-Cache") == "HIT" && rec.Code == http.StatusOK
	assert.False(t, replayed, "DELETE answered 'deleted successfully (cached)' without deleting anything")
}

// ---------------------------------------------------------------------------
// High 3: keys are not scoped per client
// ---------------------------------------------------------------------------

func TestKnownBug_IdempotencyKeyIsScopedPerClient(t *testing.T) {
	b := &fakeBackend{}
	s := newTestServer(b)
	do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":10}`, map[string]string{"Idempotency-Key": "1", "X-Client-ID": "A"})
	rec := do(t, s.handlePut, http.MethodPost, "/kv", `{"key":2,"value":20}`, map[string]string{"Idempotency-Key": "1", "X-Client-ID": "B"})

	assert.Equal(t, "MISS", rec.Header().Get("X-Cache"), "client B's write was swallowed by client A's key")
	assert.EqualValues(t, 2, b.forwardCnt.Load())
}

func TestKnownBug_ClientIdentityDoesNotIncludeEphemeralPort(t *testing.T) {
	mk := func(remote string) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/kv", nil)
		r.RemoteAddr = remote
		return r
	}
	assert.Equal(t, getClientID(mk("10.0.0.5:51000")), getClientID(mk("10.0.0.5:51001")),
		"the same host must keep one identity across connections")
	assert.NotEqual(t, getClientID(mk("10.0.0.5:51000")), getClientID(mk("10.0.0.6:51000")))
}

// ---------------------------------------------------------------------------
// Medium: in-flight duplicates
// ---------------------------------------------------------------------------

func TestKnownBug_InFlightDuplicatesAreProposedOnce(t *testing.T) {
	b := &fakeBackend{forwardFn: func(context.Context, *raftpb.Message) (*raftpb.ForwardResponse, error) {
		time.Sleep(100 * time.Millisecond)
		return &raftpb.ForwardResponse{Success: true}, nil
	}}
	s := newTestServer(b)
	var wg sync.WaitGroup
	codes := make([]int, 10)
	for i := range codes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i] = do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":1}`,
				map[string]string{"Idempotency-Key": "same", "X-Client-ID": "A"}).Code
		}(i)
	}
	wg.Wait()

	assert.EqualValues(t, 1, b.forwardCnt.Load(), "10 concurrent requests with one key were proposed %d times", b.forwardCnt.Load())
	for _, c := range codes {
		assert.Contains(t, []int{http.StatusOK, http.StatusConflict}, c, "duplicates are either coalesced (200) or told to retry (409)")
	}
}

func TestKnownBug_RequestDeadlineSaysOutcomeIsUnknown(t *testing.T) {
	b := &fakeBackend{forwardFn: func(ctx context.Context, _ *raftpb.Message) (*raftpb.ForwardResponse, error) {
		<-ctx.Done() // the entry may still commit after the caller gave up
		return nil, ctx.Err()
	}}
	s := newTestServer(b)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/kv", strings.NewReader(`{"key":1,"value":1}`)).WithContext(ctx)
	req.Header.Set("Idempotency-Key", "k")
	rec := httptest.NewRecorder()

	s.handlePut(rec, req)

	require.Equal(t, http.StatusGatewayTimeout, rec.Code)
	assert.Contains(t, strings.ToLower(rec.Body.String()), "unknown",
		"a 504 must tell the client the write may or may not have been applied")
}

func TestKnownBug_FailedResultIsNotCachedAndNotReportedAsStored(t *testing.T) {
	var n atomic.Int32
	b := &fakeBackend{forwardFn: func(context.Context, *raftpb.Message) (*raftpb.ForwardResponse, error) {
		return &raftpb.ForwardResponse{Success: n.Add(1) > 1}, nil // first attempt fails, second succeeds
	}}
	s := newTestServer(b)
	hdr := map[string]string{"Idempotency-Key": "k", "X-Client-ID": "A"}

	first := do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":1}`, hdr)
	assert.NotContains(t, first.Body.String(), "stored successfully", "Success:false must not be reported as stored")

	second := do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":1}`, hdr)
	assert.EqualValues(t, 2, b.forwardCnt.Load(), "a failed result must not be replayed from the cache")
	assert.Contains(t, second.Body.String(), `"success":true`)
}

func TestKnownBug_ClientCancellationIsNotAnInternalError(t *testing.T) {
	b := &fakeBackend{forwardFn: func(ctx context.Context, _ *raftpb.Message) (*raftpb.ForwardResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	s := newTestServer(b)
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/kv", strings.NewReader(`{"key":1,"value":1}`)).WithContext(ctx)
	rec := httptest.NewRecorder()
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()

	s.handlePut(rec, req)

	assert.NotEqual(t, http.StatusInternalServerError, rec.Code, "a client that hung up is not a server error")
}

// ---------------------------------------------------------------------------
// Medium: unbounded, client-controlled memory; key logged verbatim
// ---------------------------------------------------------------------------

func TestKnownBug_IdempotencyCacheHasABoundedSize(t *testing.T) {
	c := NewIdempotencyCache(time.Hour)
	const inserts = 300_000
	for i := 0; i < inserts; i++ {
		c.Set(strings.Repeat("k", 8)+string(rune('a'+i%26))+strings.Repeat("x", i%7)+itoa(i), raft.BroadcastResponse{Success: true})
	}
	c.mu.RLock()
	size := len(c.entries)
	c.mu.RUnlock()
	assert.LessOrEqual(t, size, 100_000, "a client sending unique keys grew the cache to %d entries", size)
}

func itoa(i int) string {
	var b [20]byte
	pos := len(b)
	for {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
		if i == 0 {
			break
		}
	}
	return string(b[pos:])
}

func TestKnownBug_OversizedIdempotencyKeyIsRejected(t *testing.T) {
	b := &fakeBackend{}
	s := newTestServer(b)
	rec := do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":1}`,
		map[string]string{"Idempotency-Key": strings.Repeat("k", 1<<20)})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Zero(t, b.forwardCnt.Load())
}

func TestKnownBug_ClientSuppliedKeyIsNotLoggedVerbatim(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(old)

	s := newTestServer(&fakeBackend{})
	hdr := map[string]string{"Idempotency-Key": "SECRET-CLIENT-KEY-123", "X-Client-ID": "A"}
	do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":1}`, hdr)
	do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":1}`, hdr) // cache hit path logs the token

	assert.NotContains(t, buf.String(), "SECRET-CLIENT-KEY-123")
}

// ---------------------------------------------------------------------------
// Low
// ---------------------------------------------------------------------------

// The panic happens in the cleanup goroutine (time.NewTicker), where
// assert.NotPanics cannot catch it, and it would take the whole test binary
// down. So each TTL runs in a child process and must exit cleanly.
func TestKnownBug_ZeroOrNegativeTTLDoesNotCrashTheProcess(t *testing.T) {
	if v := os.Getenv("API_TTL_CHILD"); v != "" {
		d, err := time.ParseDuration(v)
		require.NoError(t, err)
		NewIdempotencyCache(d)
		time.Sleep(300 * time.Millisecond) // let the cleanup goroutine start
		return
	}
	for _, ttl := range []string{"0s", "-1s", "1ns"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestKnownBug_ZeroOrNegativeTTLDoesNotCrashTheProcess$")
		cmd.Env = append(os.Environ(), "API_TTL_CHILD="+ttl)
		out, err := cmd.CombinedOutput()
		assert.NoError(t, err, "ttl=%s crashed the process:\n%.300s", ttl, out)
	}
}

func TestKnownBug_StopStopsTheCleanupGoroutine(t *testing.T) {
	runtime.GC()
	base := runtime.NumGoroutine()
	servers := make([]*Server, 20)
	for i := range servers {
		servers[i] = newTestServer(&fakeBackend{})
	}
	for _, s := range servers {
		require.NoError(t, s.Stop())
	}
	assert.Eventually(t, func() bool { return runtime.NumGoroutine() <= base+2 },
		2*time.Second, 20*time.Millisecond, "cleanup goroutines survive Server.Stop: %d goroutines, base %d", runtime.NumGoroutine(), base)
}

// ---------------------------------------------------------------------------
// Performance
// ---------------------------------------------------------------------------

func TestKnownBug_RequestsWithoutKeyShipNoTokenIntoTheLog(t *testing.T) {
	b := &fakeBackend{}
	s := newTestServer(b)
	do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":1}`, nil)
	do(t, deleteHandler(s, 1), http.MethodDelete, "/kv/1", "", nil)
	require.Len(t, b.forwarded, 2)
	for _, m := range b.forwarded {
		assert.Empty(t, m.IdempotencyToken, "a random %d-byte token is replicated to every follower and written to disk for nothing", len(m.IdempotencyToken))
	}
}

// The periodic expiry scan must not stall readers for the whole scan.
func TestKnownBug_CleanupDoesNotStallReaders(t *testing.T) {
	const ttl = 300 * time.Millisecond
	c := NewIdempotencyCache(ttl)
	c.mu.Lock()
	old := time.Now().Add(-time.Hour)
	for i := 0; i < 1_500_000; i++ {
		c.entries[itoa(i)] = &IdempotencyCacheEntry{CompletedAt: old}
	}
	c.mu.Unlock()

	var worst time.Duration
	deadline := time.Now().Add(2 * ttl)
	for time.Now().Before(deadline) {
		start := time.Now()
		c.Get("probe")
		if d := time.Since(start); d > worst {
			worst = d
		}
		time.Sleep(100 * time.Microsecond)
	}
	assert.Less(t, worst, 10*time.Millisecond, "a Get waited %v behind the cleanup scan", worst)
}

func BenchmarkIdempotencyCacheParallelSetGet(b *testing.B) {
	c := NewIdempotencyCache(time.Hour)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := itoa(i % 4096)
			if i%4 == 0 {
				c.Set(k, raft.BroadcastResponse{Success: true})
			} else {
				c.Get(k)
			}
			i++
		}
	})
}
