package cache

import (
	"fmt"
	"sync/atomic"
	"testing"
)

// BenchmarkStore_ParallelMixed runs a 90% Get / 10% Put(+occasional Delete)
// workload over a preloaded key space on every thread-safe implementation.
func BenchmarkStore_ParallelMixed(b *testing.B) {
	const keySpace = 4096
	keys := make([]string, keySpace)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	for _, im := range impls {
		if !im.threadSafe {
			continue
		}
		b.Run(im.name, func(b *testing.B) {
			c := im.new()
			for _, k := range keys {
				c.Put(k, "value")
			}
			var seed atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				x := seed.Add(1) * 0x9E3779B97F4A7C15 // per-goroutine xorshift state
				for pb.Next() {
					x ^= x << 13
					x ^= x >> 7
					x ^= x << 17
					k := keys[x%keySpace]
					switch op := (x >> 32) % 100; {
					case op < 90:
						c.Get(k)
					case op < 98:
						c.Put(k, "value")
					default:
						c.Delete(k)
					}
				}
			})
		})
	}
}
