package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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

// fakeApp is a trivial raft.Application used for stale reads.
type fakeApp struct{ vals map[int]int }

func (f *fakeApp) AppendMessage(raft.Message) (bool, int) { return true, 0 }
func (f *fakeApp) GetSnapshot() ([]byte, error)           { return nil, nil }
func (f *fakeApp) RestoreFromSnapshot([]byte) (error, int) {
	return nil, 0
}
func (f *fakeApp) GetValue(key int) int { return f.vals[key] }

// fakeBackend implements raftBackend.
type fakeBackend struct {
	mu         sync.Mutex
	forwardFn  func(ctx context.Context, msg *raftpb.Message) (*raftpb.ForwardResponse, error)
	getFn      func(ctx context.Context, req *raftpb.GetRequest) (*raftpb.GetResponse, error)
	forwarded  []*raftpb.Message
	app        *fakeApp
	status     raft.ClusterStatus
	isLeader   bool
	forwardCnt atomic.Int32
}

func (f *fakeBackend) Forward(ctx context.Context, msg *raftpb.Message) (*raftpb.ForwardResponse, error) {
	f.forwardCnt.Add(1)
	f.mu.Lock()
	f.forwarded = append(f.forwarded, msg)
	f.mu.Unlock()
	if f.forwardFn != nil {
		return f.forwardFn(ctx, msg)
	}
	return &raftpb.ForwardResponse{Success: true}, nil
}

func (f *fakeBackend) ForwardGet(ctx context.Context, req *raftpb.GetRequest) (*raftpb.GetResponse, error) {
	if f.getFn != nil {
		return f.getFn(ctx, req)
	}
	return &raftpb.GetResponse{Key: req.Key, Found: false}, nil
}

func (f *fakeBackend) GetApplication() raft.Application { return f.app }
func (f *fakeBackend) GetStatus() raft.ClusterStatus    { return f.status }
func (f *fakeBackend) IsLeader() bool                   { return f.isLeader }

func newTestServer(b *fakeBackend) *Server {
	s := &Server{
		raft:             b,
		retrier:          NewRetrier(RetryConfig{MaxAttempts: 3, InitialDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond, Multiplier: 2}),
		idempotencyCache: NewIdempotencyCache(time.Minute),
		leaderCache:      NewLeaderCache(time.Second),
	}
	return s
}

func do(t *testing.T, h http.HandlerFunc, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

func serve(s *Server, req *http.Request) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv", s.handleKV)
	mux.HandleFunc("/kv/", s.handleKVWithKey)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestHandlePut_DuplicateIdempotencyKeyReplaysCachedResponse(t *testing.T) {
	b := &fakeBackend{}
	s := newTestServer(b)
	hdr := map[string]string{"Idempotency-Key": "tok-1"}

	first := do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":10}`, hdr)
	require.Equal(t, http.StatusOK, first.Code)
	assert.Equal(t, "MISS", first.Header().Get("X-Cache"))

	second := do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":10}`, hdr)
	require.Equal(t, http.StatusOK, second.Code)
	assert.Equal(t, "HIT", second.Header().Get("X-Cache"))

	var resp PutResponse
	require.NoError(t, json.Unmarshal(second.Body.Bytes(), &resp))
	assert.True(t, resp.Success)
	assert.Equal(t, int32(1), b.forwardCnt.Load(), "duplicate must be forwarded to raft exactly once")
	assert.Equal(t, "tok-1", b.forwarded[0].IdempotencyToken)
}

func TestHandlePut_DifferentIdempotencyKeysDoNotCollide(t *testing.T) {
	b := &fakeBackend{}
	s := newTestServer(b)

	for _, tok := range []string{"a", "b"} {
		rec := do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":10}`, map[string]string{"Idempotency-Key": tok})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "MISS", rec.Header().Get("X-Cache"), "token %s", tok)
	}
	assert.Equal(t, int32(2), b.forwardCnt.Load())
}

func TestHandlePut_PutAndDeleteTokensShareNamespaceOnlyWhenEqual(t *testing.T) {
	b := &fakeBackend{}
	s := newTestServer(b)
	hdr := map[string]string{"Idempotency-Key": "same"}

	do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":1}`, hdr)
	rec := do(t, func(w http.ResponseWriter, r *http.Request) { s.handleDelete(w, r, 1) }, http.MethodDelete, "/kv/1", "", hdr)
	// Documented behaviour: the cache is keyed by token only, so reusing a
	// token across operations replays the PUT response for the DELETE.
	assert.Equal(t, "HIT", rec.Header().Get("X-Cache"))
	assert.Equal(t, int32(1), b.forwardCnt.Load())
}

// Without an Idempotency-Key header the server must not dedupe distinct
// logical requests: PUT k=5, PUT k=6, PUT k=5 must apply all three writes.
func TestHandlePut_NoHeaderDoesNotSwallowLaterIdenticalWrite(t *testing.T) {
	b := &fakeBackend{}
	s := newTestServer(b)
	hdr := map[string]string{"X-Client-ID": "c1"}

	for _, body := range []string{`{"key":1,"value":5}`, `{"key":1,"value":6}`, `{"key":1,"value":5}`} {
		rec := do(t, s.handlePut, http.MethodPost, "/kv", body, hdr)
		require.Equal(t, http.StatusOK, rec.Code)
	}
	assert.Equal(t, int32(3), b.forwardCnt.Load(), "third write (A,B,A) was swallowed by content-hash dedup => lost update")
}

func TestHandleDelete_NoHeaderDoesNotSwallowLaterDelete(t *testing.T) {
	b := &fakeBackend{}
	s := newTestServer(b)
	hdr := map[string]string{"X-Client-ID": "c1"}
	del := func(w http.ResponseWriter, r *http.Request) { s.handleDelete(w, r, 7) }

	do(t, del, http.MethodDelete, "/kv/7", "", hdr)
	do(t, s.handlePut, http.MethodPost, "/kv", `{"key":7,"value":1}`, hdr)
	do(t, del, http.MethodDelete, "/kv/7", "", hdr)

	assert.Equal(t, int32(3), b.forwardCnt.Load(), "second DELETE after re-PUT was replayed from cache")
}

func TestHandleDelete_DuplicateKeyReplaysAndForwardsOnce(t *testing.T) {
	b := &fakeBackend{}
	s := newTestServer(b)
	hdr := map[string]string{"Idempotency-Key": "d1"}
	del := func(w http.ResponseWriter, r *http.Request) { s.handleDelete(w, r, 3) }

	first := do(t, del, http.MethodDelete, "/kv/3", "", hdr)
	second := do(t, del, http.MethodDelete, "/kv/3", "", hdr)

	assert.Equal(t, "MISS", first.Header().Get("X-Cache"))
	assert.Equal(t, "HIT", second.Header().Get("X-Cache"))
	assert.Equal(t, int32(1), b.forwardCnt.Load())
	assert.Equal(t, raftpb.Message_DELETE, b.forwarded[0].Type)
	assert.EqualValues(t, 3, b.forwarded[0].Key)
}

func TestHandlePut_BadBody(t *testing.T) {
	b := &fakeBackend{}
	s := newTestServer(b)
	rec := do(t, s.handlePut, http.MethodPost, "/kv", `not json`, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Zero(t, b.forwardCnt.Load())
}

func TestHandlePut_ContextTimeoutReturns504AndDoesNotCache(t *testing.T) {
	var sawCancel atomic.Bool
	b := &fakeBackend{forwardFn: func(ctx context.Context, _ *raftpb.Message) (*raftpb.ForwardResponse, error) {
		<-ctx.Done() // the proposal is pending until the request context ends
		sawCancel.Store(true)
		return nil, errors.New("timeout waiting for commit")
	}}
	s := newTestServer(b)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/kv", strings.NewReader(`{"key":1,"value":1}`)).WithContext(ctx)
	req.Header.Set("Idempotency-Key", "slow")
	rec := httptest.NewRecorder()

	start := time.Now()
	s.handlePut(rec, req)

	assert.Equal(t, http.StatusGatewayTimeout, rec.Code)
	assert.Less(t, time.Since(start), 2*time.Second, "handler must honor request deadline, not the 15s internal one")
	assert.True(t, sawCancel.Load(), "pending proposal must observe cancellation")
	_, cached := s.idempotencyCache.Get("slow")
	assert.False(t, cached, "failed request must not populate idempotency cache")
}

func TestHandleDelete_ContextTimeoutReturns504(t *testing.T) {
	b := &fakeBackend{forwardFn: func(ctx context.Context, _ *raftpb.Message) (*raftpb.ForwardResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	s := newTestServer(b)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodDelete, "/kv/1", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	s.handleDelete(rec, req, 1)
	assert.Equal(t, http.StatusGatewayTimeout, rec.Code)
	assert.Zero(t, len(s.idempotencyCache.entries))
}

func TestHandlePut_NoLeaderMapsTo503(t *testing.T) {
	b := &fakeBackend{forwardFn: func(context.Context, *raftpb.Message) (*raftpb.ForwardResponse, error) {
		return nil, errors.New("no leader known")
	}}
	s := newTestServer(b)
	rec := do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":1}`, map[string]string{"Idempotency-Key": "nl"})
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.EqualValues(t, 3, b.forwardCnt.Load(), "retrier should attempt MaxAttempts times")
	_, cached := s.idempotencyCache.Get("nl")
	assert.False(t, cached)
}

func TestHandlePut_TransientErrorRetriedThenSucceeds(t *testing.T) {
	var n atomic.Int32
	b := &fakeBackend{forwardFn: func(context.Context, *raftpb.Message) (*raftpb.ForwardResponse, error) {
		if n.Add(1) < 3 {
			return nil, errors.New("election in progress")
		}
		return &raftpb.ForwardResponse{Success: true}, nil
	}}
	s := newTestServer(b)
	rec := do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":1}`, map[string]string{"Idempotency-Key": "r"})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.EqualValues(t, 3, n.Load())
	// every attempt must carry the same token so downstream can dedupe
	for _, m := range b.forwarded {
		assert.Equal(t, "r", m.IdempotencyToken)
	}
}

func TestHandlePut_NonRetryableErrorReturns500Once(t *testing.T) {
	b := &fakeBackend{forwardFn: func(context.Context, *raftpb.Message) (*raftpb.ForwardResponse, error) {
		return nil, errors.New("invalid state")
	}}
	s := newTestServer(b)
	rec := do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":1}`, nil)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.EqualValues(t, 1, b.forwardCnt.Load())
}

func TestHandlePut_ConcurrentSameKeyRace(t *testing.T) {
	b := &fakeBackend{}
	s := newTestServer(b)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := do(t, s.handlePut, http.MethodPost, "/kv", `{"key":1,"value":1}`, map[string]string{"Idempotency-Key": "k"})
			assert.Equal(t, http.StatusOK, rec.Code)
		}(i)
	}
	wg.Wait()
	// Known limitation: no in-flight dedup, so concurrent duplicates may each
	// reach raft; once one completes the rest are served from cache.
	assert.GreaterOrEqual(t, b.forwardCnt.Load(), int32(1))
	_, cached := s.idempotencyCache.Get("k")
	assert.True(t, cached)
}

func TestHandleGet_Routes(t *testing.T) {
	b := &fakeBackend{
		app: &fakeApp{vals: map[int]int{4: 44}},
		getFn: func(_ context.Context, req *raftpb.GetRequest) (*raftpb.GetResponse, error) {
			return &raftpb.GetResponse{Key: req.Key, Value: 99, Found: true}, nil
		},
	}
	s := newTestServer(b)

	rec := serve(s, httptest.NewRequest(http.MethodGet, "/kv/4", nil))
	var g GetResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &g))
	assert.Equal(t, GetResponse{Key: 4, Value: 99, Found: true}, g, "linearizable path goes through ForwardGet")

	rec = serve(s, httptest.NewRequest(http.MethodGet, "/kv/4?stale=true", nil))
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &g))
	assert.Equal(t, 44, g.Value, "stale path reads local application")
}

func TestHandleGet_ErrorsAndTimeout(t *testing.T) {
	b := &fakeBackend{getFn: func(ctx context.Context, _ *raftpb.GetRequest) (*raftpb.GetResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	s := newTestServer(b)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	rec := serve(s, httptest.NewRequest(http.MethodGet, "/kv/1", nil).WithContext(ctx))
	assert.Equal(t, http.StatusGatewayTimeout, rec.Code)

	b.getFn = func(context.Context, *raftpb.GetRequest) (*raftpb.GetResponse, error) {
		return nil, errors.New("boom")
	}
	rec = serve(s, httptest.NewRequest(http.MethodGet, "/kv/1", nil))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestRouting_InvalidKeyAndMethods(t *testing.T) {
	s := newTestServer(&fakeBackend{})
	assert.Equal(t, http.StatusBadRequest, serve(s, httptest.NewRequest(http.MethodGet, "/kv/abc", nil)).Code)
	assert.Equal(t, http.StatusMethodNotAllowed, serve(s, httptest.NewRequest(http.MethodPut, "/kv/1", nil)).Code)
	assert.Equal(t, http.StatusMethodNotAllowed, serve(s, httptest.NewRequest(http.MethodGet, "/kv", nil)).Code)
}

func TestStatusLeaderHealthHandlers(t *testing.T) {
	b := &fakeBackend{isLeader: true, status: raft.ClusterStatus{NodeID: 2, Role: "leader", Term: 3, LeaderID: 2, TotalNodes: 3}}
	s := newTestServer(b)

	rec := do(t, s.handleStatus, http.MethodGet, "/status", "", nil)
	var st StatusResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &st))
	assert.Equal(t, StatusResponse{NodeID: 2, Role: "leader", Term: 3, LeaderID: 2, TotalNodes: 3}, st)

	rec = do(t, s.handleLeader, http.MethodGet, "/leader", "", nil)
	var l LeaderResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &l))
	assert.True(t, l.IsLeader)
	assert.Equal(t, 2, l.LeaderID)

	rec = do(t, s.handleHealth, http.MethodGet, "/health", "", nil)
	assert.Contains(t, rec.Body.String(), "healthy")
}
