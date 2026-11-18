package client

import (
	"fmt"
	"testing"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
)

func benchClient(shards, nodes int) *SmartClient {
	cfg := &metadata.ClusterConfig{
		Epoch:       1,
		TotalShards: shards,
		Nodes:       map[string]metadata.NodeMetadata{},
		Shards:      map[int]metadata.ShardMetadata{},
	}
	for n := 0; n < nodes; n++ {
		id := fmt.Sprintf("n%d", n)
		cfg.Nodes[id] = metadata.NodeMetadata{ID: id, Address: id + ":9000", Status: metadata.StatusActive}
	}
	for s := 0; s < shards; s++ {
		cfg.Shards[s] = metadata.ShardMetadata{ID: s, PrimaryID: fmt.Sprintf("n%d", s%nodes)}
	}
	return &SmartClient{config: cfg}
}

func BenchmarkSmartClient_Route(b *testing.B) {
	for _, shards := range []int{10, 256} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			c := benchClient(shards, 3)
			keys := make([]string, 1024)
			for i := range keys {
				keys[i] = fmt.Sprintf("user:%d", i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := c.Route(keys[i&1023]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSmartClient_Route_Parallel(b *testing.B) {
	c := benchClient(64, 3)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			if _, err := c.Route(fmt.Sprint("k", i&255)); err != nil {
				b.Fatal(err)
			}
		}
	})
}
