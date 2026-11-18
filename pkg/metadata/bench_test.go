package metadata

import (
	"fmt"
	"testing"
)

func benchConfig(shards, nodes int) *ClusterConfig {
	cfg := NewClusterConfig(shards)
	ids := make([]string, nodes)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%d", i)
		cfg.Nodes[ids[i]] = NodeMetadata{ID: ids[i], Address: ids[i] + ":1", Status: StatusActive}
	}
	for s := 0; s < shards; s++ {
		cfg.Shards[s] = ShardMetadata{
			ID:         s,
			PrimaryID:  ids[s%nodes],
			ReplicaIDs: []string{ids[(s+1)%nodes], ids[(s+2)%nodes]},
			Status:     ShardStatusActive,
		}
	}
	return cfg
}

func BenchmarkRebalance(b *testing.B) {
	for _, shards := range []int{16, 256, 4096} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			cfg := benchConfig(shards, 5)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Rebalance(cfg, "n0")
			}
		})
	}
}
