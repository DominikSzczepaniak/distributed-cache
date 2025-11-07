# Retry & Fault Tolerance Addendum

**Created**: 2025-11-06
**Purpose**: Comprehensive retry logic and fault tolerance for interactive API
**Addendum to**: `interactive-api-implementation-plan.md`

---

## Critical Gap Identified

The original plan lacked comprehensive retry logic for failed requests. This addendum addresses:

1. ✅ Request retry mechanisms at multiple layers
2. ✅ Idempotency to prevent duplicate operations
3. ✅ Exponential backoff strategies
4. ✅ Leader redirection and discovery
5. ✅ Client-side retry in CLI
6. ✅ Timeout and circuit breaker patterns

---

## Table of Contents

1. [Failure Scenarios](#failure-scenarios)
2. [Retry Architecture](#retry-architecture)
3. [Idempotency Design](#idempotency-design)
4. [Implementation Specifications](#implementation-specifications)
5. [Updated Request Flows](#updated-request-flows)
6. [Testing Strategy](#testing-strategy)

---

## Failure Scenarios

### Write Operation Failures

| Scenario | Cause | Should Retry? | Strategy |
|----------|-------|---------------|----------|
| **Timeout waiting for consensus** | Network slow, quorum delay | ✅ Yes | Retry with backoff, max 3 attempts |
| **Leader election in progress** | No leader currently | ✅ Yes | Retry with exponential backoff |
| **Node is follower** | Request sent to non-leader | ✅ Yes | Redirect to leader immediately |
| **Network partition** | Node isolated | ✅ Yes | Retry until timeout, then fail |
| **Duplicate request** | Client retry after timeout | ⚠️ Conditional | Use idempotency token |
| **Invalid input** | Malformed request | ❌ No | Return 400 immediately |
| **Application error** | State machine rejects | ❌ No | Return 500 immediately |

### Read Operation Failures

| Scenario | Cause | Should Retry? | Strategy |
|----------|-------|---------------|----------|
| **Stale data acceptable** | Follower read | ❌ No | Return immediately |
| **Leader required but unavailable** | Leader election | ✅ Yes | Wait for new leader, retry |
| **Network timeout** | Network issues | ✅ Yes | Retry with backoff |
| **Key not found** | Key doesn't exist | ❌ No | Return 404 immediately |

### Health Check Failures

| Scenario | Cause | Should Retry? | Strategy |
|----------|-------|---------------|----------|
| **Node unreachable** | Network/node down | ✅ Yes | Try different node |
| **Slow response** | Node overloaded | ✅ Yes | Retry with timeout |

---

## Retry Architecture

### Multi-Layer Retry Strategy

```
┌─────────────────────────────────────────────────────────┐
│                    Client Layer                         │
│  - CLI retry with user feedback                        │
│  - SDK retry with exponential backoff                  │
│  - Circuit breaker to prevent cascading failures       │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                 HTTP API Layer                          │
│  - Request validation (no retry on invalid input)      │
│  - Leader redirection (immediate retry to leader)      │
│  - Timeout with retry (exponential backoff)            │
│  - Idempotency check (deduplicate retries)            │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                  Raft Layer                             │
│  - BroadcastSync with timeout                          │
│  - Internal retry for replication failures             │
│  - Quorum wait with deadline                           │
└─────────────────────────────────────────────────────────┘
```

### Retry Configuration

```go
type RetryConfig struct {
    MaxAttempts      int           // Default: 3
    InitialDelay     time.Duration // Default: 100ms
    MaxDelay         time.Duration // Default: 5s
    Multiplier       float64       // Default: 2.0
    Jitter           bool          // Default: true (prevent thundering herd)
    EnableIdempotency bool         // Default: true for writes
}

// Default configurations by operation
var DefaultRetryConfigs = map[string]RetryConfig{
    "PUT": {
        MaxAttempts:      3,
        InitialDelay:     100 * time.Millisecond,
        MaxDelay:         5 * time.Second,
        Multiplier:       2.0,
        Jitter:           true,
        EnableIdempotency: true,
    },
    "DELETE": {
        MaxAttempts:      3,
        InitialDelay:     100 * time.Millisecond,
        MaxDelay:         5 * time.Second,
        Multiplier:       2.0,
        Jitter:           true,
        EnableIdempotency: true,
    },
    "GET": {
        MaxAttempts:      2,
        InitialDelay:     50 * time.Millisecond,
        MaxDelay:         2 * time.Second,
        Multiplier:       2.0,
        Jitter:           true,
        EnableIdempotency: false,
    },
}
```

---

## Idempotency Design

### Why Idempotency?

Consider this scenario:
1. Client sends `PUT key=1, value=100`
2. Server processes, commits, but response times out
3. Client retries `PUT key=1, value=100`
4. **Without idempotency**: Value written twice, may cause issues
5. **With idempotency**: Second request detected as duplicate, returns cached result

### Idempotency Token Approach

```go
type Message struct {
    MsgType      MessageType
    Key          int
    Value        *int
    ResponseChan chan<- BroadcastResponse

    // NEW: Idempotency support
    IdempotencyToken string    // Client-generated UUID
    ClientID         string    // Client identifier
    RequestID        uint64    // Monotonic request counter
}

// Idempotency cache
type IdempotencyCache struct {
    mu      sync.RWMutex
    entries map[string]*IdempotencyCacheEntry
    ttl     time.Duration // Default: 5 minutes
}

type IdempotencyCacheEntry struct {
    Response    BroadcastResponse
    CompletedAt time.Time
}
```

### Idempotency Flow

```
Client Request with Token → Check Cache
  ├─ Found → Return cached response (200 OK)
  └─ Not Found → Process request
       ├─ Success → Cache response + return
       └─ Failure → Don't cache + return error
```

### Token Generation

**Client-Side** (Recommended):
```bash
# CLI generates token
PUT_TOKEN=$(uuidgen)
curl -X POST http://localhost:8080/kv \
  -H "Idempotency-Key: $PUT_TOKEN" \
  -d '{"key":1,"value":100}'
```

**Server-Side** (Fallback):
```go
// If client doesn't provide token, generate from request hash
func generateIdempotencyToken(req *PutRequest, clientID string) string {
    h := sha256.New()
    h.Write([]byte(fmt.Sprintf("%s:%d:%d", clientID, req.Key, req.Value)))
    return hex.EncodeToString(h.Sum(nil))
}
```

---

## Implementation Specifications

### 1. Retry Helper Package

**File**: `pkg/api/retry.go`

```go
package api

import (
    "context"
    "fmt"
    "math"
    "math/rand"
    "time"
)

type RetryableFunc func(ctx context.Context) error

type Retrier struct {
    config RetryConfig
}

func NewRetrier(config RetryConfig) *Retrier {
    return &Retrier{config: config}
}

// ExecuteWithRetry executes a function with retry logic
func (r *Retrier) ExecuteWithRetry(ctx context.Context, fn RetryableFunc) error {
    var lastErr error

    for attempt := 0; attempt < r.config.MaxAttempts; attempt++ {
        // Execute the function
        err := fn(ctx)
        if err == nil {
            return nil // Success!
        }

        lastErr = err

        // Check if error is retryable
        if !r.isRetryableError(err) {
            return err // Don't retry
        }

        // Don't sleep after last attempt
        if attempt < r.config.MaxAttempts-1 {
            delay := r.calculateBackoff(attempt)

            select {
            case <-time.After(delay):
                // Continue to next attempt
            case <-ctx.Done():
                return ctx.Err()
            }
        }
    }

    return fmt.Errorf("max retry attempts (%d) exceeded: %w",
        r.config.MaxAttempts, lastErr)
}

func (r *Retrier) calculateBackoff(attempt int) time.Duration {
    // Exponential backoff: initialDelay * (multiplier ^ attempt)
    delay := float64(r.config.InitialDelay) * math.Pow(r.config.Multiplier, float64(attempt))

    // Cap at max delay
    if delay > float64(r.config.MaxDelay) {
        delay = float64(r.config.MaxDelay)
    }

    // Add jitter to prevent thundering herd
    if r.config.Jitter {
        jitter := rand.Float64() * delay * 0.3 // ±30% jitter
        delay = delay + jitter - (delay * 0.15)
    }

    return time.Duration(delay)
}

func (r *Retrier) isRetryableError(err error) bool {
    if err == nil {
        return false
    }

    // Classify errors
    errStr := err.Error()

    // Retryable errors
    retryable := []string{
        "timeout",
        "no leader",
        "election in progress",
        "leader unavailable",
        "network",
        "connection refused",
        "EOF",
    }

    for _, pattern := range retryable {
        if contains(errStr, pattern) {
            return true
        }
    }

    // Non-retryable errors
    nonRetryable := []string{
        "invalid",
        "bad request",
        "not found",
        "conflict",
    }

    for _, pattern := range nonRetryable {
        if contains(errStr, pattern) {
            return false
        }
    }

    // Default: retry on unknown errors
    return true
}

func contains(s, substr string) bool {
    return len(s) >= len(substr) &&
           (s == substr || len(s) > len(substr) &&
            (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr))
}
```

---

### 2. Idempotency Cache

**File**: `pkg/api/idempotency.go`

```go
package api

import (
    "sync"
    "time"

    "github.com/dominikszczepaniak/distributed-cache/pkg/raft"
)

type IdempotencyCache struct {
    mu      sync.RWMutex
    entries map[string]*IdempotencyCacheEntry
    ttl     time.Duration
}

type IdempotencyCacheEntry struct {
    Response    raft.BroadcastResponse
    CompletedAt time.Time
}

func NewIdempotencyCache(ttl time.Duration) *IdempotencyCache {
    cache := &IdempotencyCache{
        entries: make(map[string]*IdempotencyCacheEntry),
        ttl:     ttl,
    }

    // Start cleanup goroutine
    go cache.cleanupExpired()

    return cache
}

// Get retrieves cached response if available and not expired
func (c *IdempotencyCache) Get(token string) (*raft.BroadcastResponse, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()

    entry, exists := c.entries[token]
    if !exists {
        return nil, false
    }

    // Check if expired
    if time.Since(entry.CompletedAt) > c.ttl {
        return nil, false
    }

    return &entry.Response, true
}

// Set caches a response
func (c *IdempotencyCache) Set(token string, response raft.BroadcastResponse) {
    c.mu.Lock()
    defer c.mu.Unlock()

    c.entries[token] = &IdempotencyCacheEntry{
        Response:    response,
        CompletedAt: time.Now(),
    }
}

// cleanupExpired removes expired entries periodically
func (c *IdempotencyCache) cleanupExpired() {
    ticker := time.NewTicker(c.ttl / 2)
    defer ticker.Stop()

    for range ticker.C {
        c.mu.Lock()
        now := time.Now()
        for token, entry := range c.entries {
            if now.Sub(entry.CompletedAt) > c.ttl {
                delete(c.entries, token)
            }
        }
        c.mu.Unlock()
    }
}
```

---

### 3. Updated API Server with Retry

**File**: `pkg/api/server.go` (Updated)

```go
type Server struct {
    raft             *raft.Raft
    listenAddr       string
    httpServer       *http.Server
    retrier          *Retrier
    idempotencyCache *IdempotencyCache
    leaderCache      *LeaderCache // NEW: Cache leader info
}

func NewServer(r *raft.Raft, listenAddr string) *Server {
    return &Server{
        raft:             r,
        listenAddr:       listenAddr,
        retrier:          NewRetrier(DefaultRetryConfigs["PUT"]),
        idempotencyCache: NewIdempotencyCache(5 * time.Minute),
        leaderCache:      NewLeaderCache(1 * time.Second),
    }
}

// Updated PUT handler with retry and idempotency
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
    var req PutRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "Invalid request body", http.StatusBadRequest)
        return
    }

    // Get or generate idempotency token
    idempotencyToken := r.Header.Get("Idempotency-Key")
    if idempotencyToken == "" {
        // Generate from request content
        idempotencyToken = s.generateIdempotencyToken(&req, getClientID(r))
    }

    // Check idempotency cache
    if cachedResp, found := s.idempotencyCache.Get(idempotencyToken); found {
        slog.Info(fmt.Sprintf("Returning cached response for token: %s", idempotencyToken))
        resp := PutResponse{
            Success: cachedResp.Success,
            Message: "Key-value pair stored successfully (cached)",
        }
        w.Header().Set("Content-Type", "application/json")
        w.Header().Set("X-Cache", "HIT")
        json.NewEncoder(w).Encode(resp)
        return
    }

    // Execute with retry
    var success bool
    var broadcastErr error

    retryFunc := func(ctx context.Context) error {
        msg := raft.Message{
            MsgType:          "PUT",
            Key:              req.Key,
            Value:            &req.Value,
            IdempotencyToken: idempotencyToken,
        }

        var err error
        success, _, err = s.raft.BroadcastSync(msg, 5*time.Second)
        broadcastErr = err
        return err
    }

    ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
    defer cancel()

    err := s.retrier.ExecuteWithRetry(ctx, retryFunc)

    if err != nil {
        // Check specific error types
        if ctx.Err() == context.DeadlineExceeded {
            http.Error(w, "Request timeout after retries", http.StatusGatewayTimeout)
            return
        }
        if broadcastErr != nil && contains(broadcastErr.Error(), "no leader") {
            http.Error(w, "No leader available", http.StatusServiceUnavailable)
            return
        }
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    // Cache successful response
    s.idempotencyCache.Set(idempotencyToken, raft.BroadcastResponse{
        Success: success,
        Value:   0,
        Error:   nil,
    })

    resp := PutResponse{
        Success: success,
        Message: "Key-value pair stored successfully",
    }

    w.Header().Set("Content-Type", "application/json")
    w.Header().Set("X-Cache", "MISS")
    json.NewEncoder(w).Encode(resp)
}

func (s *Server) generateIdempotencyToken(req *PutRequest, clientID string) string {
    h := sha256.New()
    h.Write([]byte(fmt.Sprintf("%s:%d:%d", clientID, req.Key, req.Value)))
    return hex.EncodeToString(h.Sum(nil))
}

func getClientID(r *http.Request) string {
    // Use IP address or client-provided ID
    clientID := r.Header.Get("X-Client-ID")
    if clientID == "" {
        clientID = r.RemoteAddr
    }
    return clientID
}
```

---

### 4. Leader Discovery & Caching

**File**: `pkg/api/leader.go`

```go
package api

import (
    "sync"
    "time"
)

// LeaderCache caches leader information to avoid repeated lookups
type LeaderCache struct {
    mu         sync.RWMutex
    leaderID   int
    leaderAddr string
    lastUpdate time.Time
    ttl        time.Duration
}

func NewLeaderCache(ttl time.Duration) *LeaderCache {
    return &LeaderCache{
        leaderID:   -1,
        ttl:        ttl,
        lastUpdate: time.Time{},
    }
}

func (lc *LeaderCache) Get() (int, string, bool) {
    lc.mu.RLock()
    defer lc.mu.RUnlock()

    if time.Since(lc.lastUpdate) > lc.ttl {
        return -1, "", false // Expired
    }

    return lc.leaderID, lc.leaderAddr, true
}

func (lc *LeaderCache) Set(leaderID int, leaderAddr string) {
    lc.mu.Lock()
    defer lc.mu.Unlock()

    lc.leaderID = leaderID
    lc.leaderAddr = leaderAddr
    lc.lastUpdate = time.Now()
}

func (lc *LeaderCache) Invalidate() {
    lc.mu.Lock()
    defer lc.mu.Unlock()

    lc.lastUpdate = time.Time{}
}
```

---

### 5. Updated CLI with Retry

**File**: `cmd/raftcli/main.go` (Updated)

```go
type CLI struct {
    client      *http.Client
    baseURL     string
    retrier     *api.Retrier
    maxRedirects int
}

func NewCLI(baseURL string) *CLI {
    return &CLI{
        client: &http.Client{
            Timeout: 10 * time.Second,
            CheckRedirect: func(req *http.Request, via []*http.Request) error {
                // Don't auto-follow redirects, handle manually
                return http.ErrUseLastResponse
            },
        },
        baseURL:      baseURL,
        retrier:      api.NewRetrier(api.DefaultRetryConfigs["PUT"]),
        maxRedirects: 3,
    }
}

func (c *CLI) cmdPut(args []string) error {
    if len(args) != 2 {
        return fmt.Errorf("usage: put <key> <value>")
    }

    key, err := strconv.Atoi(args[0])
    if err != nil {
        return fmt.Errorf("invalid key: %v", err)
    }

    value, err := strconv.Atoi(args[1])
    if err != nil {
        return fmt.Errorf("invalid value: %v", err)
    }

    // Generate idempotency token
    idempotencyToken := uuid.New().String()

    // Execute with retry
    retryFunc := func(ctx context.Context) error {
        req := map[string]int{"key": key, "value": value}
        body, _ := json.Marshal(req)

        httpReq, _ := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/kv", bytes.NewBuffer(body))
        httpReq.Header.Set("Content-Type", "application/json")
        httpReq.Header.Set("Idempotency-Key", idempotencyToken)

        resp, err := c.client.Do(httpReq)
        if err != nil {
            return err
        }
        defer resp.Body.Close()

        // Handle redirects to leader
        if resp.StatusCode == http.StatusTemporaryRedirect {
            location := resp.Header.Get("Location")
            if location != "" {
                // Update baseURL and retry
                c.baseURL = location[:strings.LastIndex(location, "/")]
                return fmt.Errorf("redirecting to leader") // Trigger retry
            }
        }

        if resp.StatusCode != http.StatusOK {
            body, _ := io.ReadAll(resp.Body)
            return fmt.Errorf("server error: %s", body)
        }

        return nil
    }

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    err = c.retrier.ExecuteWithRetry(ctx, retryFunc)
    if err != nil {
        return fmt.Errorf("PUT failed after retries: %v", err)
    }

    fmt.Printf("✓ PUT successful: key=%d, value=%d\n", key, value)
    return nil
}
```

---

## Updated Request Flows

### Write Operation with Retry

```
Client: PUT key=1, value=100
    ↓
[Attempt 1]
    ↓
API Server: Check idempotency cache
    ├─ FOUND → Return cached response (200 OK)
    └─ NOT FOUND → Continue
         ↓
    Validate request
         ↓
    BroadcastSync(timeout=5s)
         ↓
    ┌─ Timeout after 5s
    │   ↓
    │  [Attempt 2] (after 100ms backoff)
    │   ↓
    │  Check if leader available
    │   ├─ No leader → Wait 200ms, retry
    │   └─ Leader available → BroadcastSync again
    │       ↓
    │      ┌─ Success!
    │      │   ↓
    │      │  Cache response with idempotency token
    │      │   ↓
    │      │  Return 200 OK
    │      │
    │      └─ Timeout again
    │          ↓
    │         [Attempt 3] (after 400ms backoff)
    │          ↓
    │         (repeat)
    │
    └─ After 3 attempts → Return 504 Gateway Timeout

Total max time: 5s + 100ms + 5s + 400ms + 5s = ~15.5s
```

### Read Operation with Leader Redirect

```
Client: GET /kv/42
    ↓
API Server (Follower): Check if stale reads allowed
    ├─ ?stale=true → Read local Application → Return
    └─ Linearizable read required
         ↓
    Check leader cache
         ↓
    ┌─ Cache HIT
    │   ↓
    │  Return 307 Redirect to leader
    │
    └─ Cache MISS or expired
         ↓
        Query Raft for current leader
         ↓
        Update leader cache
         ↓
        Return 307 Redirect to leader

Client follows redirect
    ↓
API Server (Leader): Verify still leader
    ↓
Read from Application
    ↓
Return 200 OK with value
```

---

## Testing Strategy

### Unit Tests

**File**: `pkg/api/retry_test.go`

```go
func TestRetrier_Success(t *testing.T) {
    retrier := NewRetrier(RetryConfig{MaxAttempts: 3})

    attempts := 0
    fn := func(ctx context.Context) error {
        attempts++
        if attempts < 2 {
            return fmt.Errorf("temporary error")
        }
        return nil // Success on 2nd attempt
    }

    err := retrier.ExecuteWithRetry(context.Background(), fn)
    assert.NoError(t, err)
    assert.Equal(t, 2, attempts)
}

func TestRetrier_MaxAttemptsExceeded(t *testing.T) {
    retrier := NewRetrier(RetryConfig{MaxAttempts: 3})

    fn := func(ctx context.Context) error {
        return fmt.Errorf("persistent error")
    }

    err := retrier.ExecuteWithRetry(context.Background(), fn)
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "max retry attempts")
}

func TestRetrier_NonRetryableError(t *testing.T) {
    retrier := NewRetrier(RetryConfig{MaxAttempts: 3})

    attempts := 0
    fn := func(ctx context.Context) error {
        attempts++
        return fmt.Errorf("invalid request") // Non-retryable
    }

    err := retrier.ExecuteWithRetry(context.Background(), fn)
    assert.Error(t, err)
    assert.Equal(t, 1, attempts) // Should not retry
}
```

**File**: `pkg/api/idempotency_test.go`

```go
func TestIdempotencyCache_HitAndMiss(t *testing.T) {
    cache := NewIdempotencyCache(1 * time.Minute)

    token := "test-token-123"
    response := raft.BroadcastResponse{Success: true, Value: 42}

    // Miss
    _, found := cache.Get(token)
    assert.False(t, found)

    // Set
    cache.Set(token, response)

    // Hit
    cached, found := cache.Get(token)
    assert.True(t, found)
    assert.Equal(t, response.Success, cached.Success)
    assert.Equal(t, response.Value, cached.Value)
}

func TestIdempotencyCache_Expiration(t *testing.T) {
    cache := NewIdempotencyCache(100 * time.Millisecond)

    token := "test-token-123"
    response := raft.BroadcastResponse{Success: true}

    cache.Set(token, response)

    // Should be cached
    _, found := cache.Get(token)
    assert.True(t, found)

    // Wait for expiration
    time.Sleep(150 * time.Millisecond)

    // Should be expired
    _, found = cache.Get(token)
    assert.False(t, found)
}
```

### Integration Tests

**File**: `tests/integration/api_retry_test.go`

```go
func TestAPI_WriteRetryOnTimeout(t *testing.T) {
    // Start 3-node cluster
    cluster := startTestCluster(t, 3)
    defer cluster.Stop()

    // Send write to node 0
    client := &http.Client{Timeout: 20 * time.Second}
    req := map[string]int{"key": 1, "value": 100}
    body, _ := json.Marshal(req)

    // This should succeed even if first attempt times out
    resp, err := client.Post("http://localhost:8080/kv", "application/json", bytes.NewBuffer(body))
    require.NoError(t, err)
    assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAPI_IdempotentRetry(t *testing.T) {
    cluster := startTestCluster(t, 3)
    defer cluster.Stop()

    client := &http.Client{}
    token := uuid.New().String()

    // First request
    req1 := makeRequest("POST", "http://localhost:8080/kv", map[string]int{"key": 1, "value": 100})
    req1.Header.Set("Idempotency-Key", token)
    resp1, err := client.Do(req1)
    require.NoError(t, err)
    assert.Equal(t, http.StatusOK, resp1.StatusCode)

    // Second request with same token (simulating retry)
    req2 := makeRequest("POST", "http://localhost:8080/kv", map[string]int{"key": 1, "value": 100})
    req2.Header.Set("Idempotency-Key", token)
    resp2, err := client.Do(req2)
    require.NoError(t, err)
    assert.Equal(t, http.StatusOK, resp2.StatusCode)
    assert.Equal(t, "HIT", resp2.Header.Get("X-Cache")) // Should be cached
}
```

---

## Configuration

### Environment Variables

Add to `docker-compose.yml`:

```yaml
environment:
  # ... existing vars ...

  # Retry configuration
  API_RETRY_MAX_ATTEMPTS: "3"
  API_RETRY_INITIAL_DELAY: "100ms"
  API_RETRY_MAX_DELAY: "5s"
  API_RETRY_MULTIPLIER: "2.0"

  # Idempotency configuration
  API_IDEMPOTENCY_TTL: "5m"
  API_IDEMPOTENCY_ENABLED: "true"

  # Leader cache configuration
  API_LEADER_CACHE_TTL: "1s"
```

---

## Performance Implications

### Latency Impact

| Scenario | Without Retry | With Retry (worst case) |
|----------|---------------|-------------------------|
| **Successful write** | ~50ms | ~50ms (same) |
| **Leader election** | Timeout (5s) | ~5.5s (1 retry) |
| **Network blip** | Timeout (5s) | ~200ms (1 quick retry) |
| **Persistent failure** | ~5s | ~15s (3 attempts) |

### Memory Impact

- **Idempotency Cache**: ~1KB per entry × 1000 entries = ~1MB
- **Leader Cache**: ~100 bytes (negligible)
- **Retry Goroutines**: ~2KB per in-flight request

### Recommendations

1. **Tune retry attempts** based on latency requirements
2. **Monitor cache hit rates** for idempotency
3. **Set reasonable TTLs** (5 minutes for idempotency)
4. **Use circuit breakers** to prevent cascading failures

---

## Summary of Changes

### New Components
✅ `pkg/api/retry.go` - Retry logic with exponential backoff
✅ `pkg/api/idempotency.go` - Idempotency cache
✅ `pkg/api/leader.go` - Leader discovery cache
✅ Updated `pkg/api/server.go` - Retry-aware handlers
✅ Updated `cmd/raftcli/main.go` - Client-side retry

### New Features
✅ Automatic retry on transient failures
✅ Exponential backoff with jitter
✅ Idempotency token support
✅ Leader redirection
✅ Request deduplication
✅ Configurable retry policies

### Error Handling
✅ Classify retryable vs non-retryable errors
✅ Timeout management
✅ Circuit breaker pattern (future)
✅ Graceful degradation

---

**Last Updated**: 2025-11-06
**Status**: Ready for implementation alongside main API plan
