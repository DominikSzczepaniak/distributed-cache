//go:build compare

package benchmark

// Optional comparison against Redis and etcd (replaces benchmark_redis.go,
// benchmark_etcd.go and benchmark_comparison.sh). Build with `-tags compare`.
// Each benchmark skips with a clear message when the server is not reachable:
//
//	docker run -d --name redis-benchmark -p 6379:6379 redis:7
//	docker run -d --name etcd-benchmark -p 12379:2379 quay.io/coreos/etcd:v3.5.11 \
//	    etcd --advertise-client-urls http://0.0.0.0:2379 --listen-client-urls http://0.0.0.0:2379
//	go test -tags compare ./tests/benchmark -run xxx -bench Compare -benchtime=2000x

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	redisAddr    = "localhost:6379"
	etcdEndpoint = "localhost:12379"
	cmpWorkers   = 100
)

func reachable(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

func BenchmarkCompareRedis(b *testing.B) {
	if !reachable(redisAddr) {
		b.Skipf("redis not reachable at %s (optional comparison; start redis to run it)", redisAddr)
	}
	rc := redis.NewClient(&redis.Options{Addr: redisAddr, PoolSize: 200})
	defer rc.Close()
	ctx := context.Background()
	if err := rc.Ping(ctx).Err(); err != nil {
		b.Skipf("redis ping failed: %v", err)
	}
	b.ResetTimer()
	report(b, runLoad(b.N, cmpWorkers, func(w, i int) error {
		return rc.Set(ctx, fmt.Sprintf("bench-%d", i), value64, 0).Err()
	}))
}

func BenchmarkCompareEtcd(b *testing.B) {
	if !reachable(etcdEndpoint) {
		b.Skipf("etcd not reachable at %s (optional comparison; start etcd to run it)", etcdEndpoint)
	}
	ec, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdEndpoint}, DialTimeout: 2 * time.Second})
	if err != nil {
		b.Skipf("etcd client: %v", err)
	}
	defer ec.Close()
	ctx := context.Background()
	b.ResetTimer()
	report(b, runLoad(b.N, cmpWorkers, func(w, i int) error {
		_, err := ec.Put(ctx, fmt.Sprintf("bench-%d", i), value64)
		return err
	}))
}
