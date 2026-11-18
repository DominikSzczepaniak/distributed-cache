package datanode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
)

func benchPut(b *testing.B, replicas int) {
	b.Helper()
	var recs []*httptest.Server
	cfg := singleShardCfg(1, metadata.ShardStatusActive)
	s := cfg.Shards[0]
	for i := 0; i < replicas; i++ {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		recs = append(recs, srv)
		id := fmt.Sprintf("r%d", i)
		cfg.Nodes[id] = metadata.NodeMetadata{ID: id, Address: srv.Listener.Addr().String(), Status: metadata.StatusActive}
		s.ReplicaIDs = append(s.ReplicaIDs, id)
	}
	cfg.Shards[0] = s
	defer func() {
		for _, r := range recs {
			r.Close()
		}
	}()

	f := newFixtureB(cfg)
	body, _ := json.Marshal(WriteRequest{Key: "bench-key", Value: "bench-value", Epoch: 1})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/data", bytes.NewReader(body))
		w := httptest.NewRecorder()
		f.srv.HandlePut(w, req)
		if w.Code != http.StatusOK {
			b.Fatalf("status %d", w.Code)
		}
	}
}

func BenchmarkHandlePut_NoReplication(b *testing.B) { benchPut(b, 0) }
func BenchmarkHandlePut_OneReplica(b *testing.B)    { benchPut(b, 1) }
func BenchmarkHandlePut_TwoReplicas(b *testing.B)   { benchPut(b, 2) }
