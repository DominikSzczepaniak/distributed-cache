package cache

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Store is the behaviour shared by every cache implementation.
type Store interface {
	Get(key string) string
	Put(key, value string)
	Delete(key string)
}

type impl struct {
	name       string
	new        func() Store
	threadSafe bool // BasicMapCache is a deliberately unsynchronised baseline.
}

var impls = []impl{
	{"ConcurrentMapCache", func() Store { return NewConcurrentMapCache() }, true},
	{"ShardedCache16", func() Store { return NewShardedCache(16) }, true},
	{"ShardedCache1", func() Store { return NewShardedCache(1) }, true},
	{"SyncMapCache", func() Store { return NewSyncMapCache() }, true},
	{"BasicMapCache", func() Store { return NewBasicMapCache() }, false},
}

func TestStore_Semantics(t *testing.T) {
	for _, im := range impls {
		t.Run(im.name, func(t *testing.T) {
			t.Run("missing key returns empty", func(t *testing.T) {
				assert.Equal(t, "", im.new().Get("nope"))
			})
			t.Run("put then get", func(t *testing.T) {
				c := im.new()
				c.Put("k", "v")
				assert.Equal(t, "v", c.Get("k"))
			})
			t.Run("overwrite", func(t *testing.T) {
				c := im.new()
				c.Put("k", "v1")
				c.Put("k", "v2")
				assert.Equal(t, "v2", c.Get("k"))
			})
			t.Run("delete removes and is idempotent", func(t *testing.T) {
				c := im.new()
				c.Put("k", "v")
				c.Delete("k")
				assert.Equal(t, "", c.Get("k"))
				c.Delete("k")
				c.Delete("never-existed")
			})
			t.Run("delete only affects its key", func(t *testing.T) {
				c := im.new()
				c.Put("a", "1")
				c.Put("b", "2")
				c.Delete("a")
				assert.Equal(t, "2", c.Get("b"))
			})
			t.Run("put after delete", func(t *testing.T) {
				c := im.new()
				c.Put("k", "v1")
				c.Delete("k")
				c.Put("k", "v2")
				assert.Equal(t, "v2", c.Get("k"))
			})
			t.Run("odd keys and values", func(t *testing.T) {
				c := im.new()
				big := strings.Repeat("x", 1<<16)
				for k, v := range map[string]string{
					"":              "empty-key",
					"ключ":          "значение",
					"with space":    "a b",
					"nul\x00inside": "nul\x00v",
					big:             big,
				} {
					c.Put(k, v)
					assert.Equal(t, v, c.Get(k), "key len %d", len(k))
				}
			})
			t.Run("many keys keep their own values", func(t *testing.T) {
				c := im.new()
				for i := 0; i < 5000; i++ {
					c.Put(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
				}
				for i := 0; i < 5000; i++ {
					require.Equal(t, fmt.Sprintf("v%d", i), c.Get(fmt.Sprintf("k%d", i)))
				}
			})
		})
	}
}

// Parallel tests are table-driven over the thread-safe implementations only;
// BasicMapCache is intentionally unsynchronised (benchmark baseline) and is
// covered by TestStore_Semantics sequentially.
func TestStore_ParallelDisjointKeys(t *testing.T) {
	for _, im := range impls {
		if !im.threadSafe {
			continue
		}
		t.Run(im.name, func(t *testing.T) {
			c := im.new()
			const workers, perWorker = 16, 300
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < perWorker; i++ {
						k := fmt.Sprintf("w%d-k%d", w, i)
						c.Put(k, k)
						if got := c.Get(k); got != k {
							t.Errorf("own write lost: key %s got %q", k, got)
							return
						}
						if i%3 == 0 {
							c.Delete(k)
							if got := c.Get(k); got != "" {
								t.Errorf("own delete not visible: key %s got %q", k, got)
								return
							}
						}
					}
				}(w)
			}
			wg.Wait()
			for w := 0; w < workers; w++ {
				for i := 0; i < perWorker; i++ {
					k := fmt.Sprintf("w%d-k%d", w, i)
					want := k
					if i%3 == 0 {
						want = ""
					}
					require.Equal(t, want, c.Get(k))
				}
			}
		})
	}
}

func TestStore_ParallelContendedKey_NoTornValues(t *testing.T) {
	for _, im := range impls {
		if !im.threadSafe {
			continue
		}
		t.Run(im.name, func(t *testing.T) {
			c := im.new()
			allowed := map[string]bool{"": true}
			for w := 0; w < 8; w++ {
				allowed[fmt.Sprintf("value-from-writer-%d", w)] = true
			}
			var wg sync.WaitGroup
			for w := 0; w < 8; w++ {
				wg.Add(2)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < 1000; i++ {
						if i%7 == 0 {
							c.Delete("hot")
						} else {
							c.Put("hot", fmt.Sprintf("value-from-writer-%d", w))
						}
					}
				}(w)
				go func() {
					defer wg.Done()
					for i := 0; i < 1000; i++ {
						if v := c.Get("hot"); !allowed[v] {
							t.Errorf("observed value nobody wrote: %q", v)
							return
						}
					}
				}()
			}
			wg.Wait()
			assert.True(t, allowed[c.Get("hot")])
		})
	}
}

func TestStore_ParallelLastWriterWinsPerKeyAfterBarrier(t *testing.T) {
	for _, im := range impls {
		if !im.threadSafe {
			continue
		}
		t.Run(im.name, func(t *testing.T) {
			c := im.new()
			var wg sync.WaitGroup
			for w := 0; w < 10; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < 200; i++ {
						c.Put("shared", fmt.Sprint(w))
					}
				}(w)
			}
			wg.Wait()
			c.Put("shared", "final")
			assert.Equal(t, "final", c.Get("shared"), "a write that happens-after all others must win")
		})
	}
}

func TestShardedCache_ShardCountNormalisation(t *testing.T) {
	for _, tc := range []struct{ in, want int }{
		{0, 16}, {-4, 16}, {3, 16}, {6, 16}, {1, 1}, {2, 2}, {8, 8}, {64, 64},
	} {
		sc := NewShardedCache(tc.in)
		assert.Equal(t, tc.want, sc.numShards, "NewShardedCache(%d)", tc.in)
		assert.Len(t, sc.shards, tc.want)
		assert.Equal(t, tc.want-1, sc.shardMask)
	}
}

func TestShardedCache_KeysSpreadAcrossShards(t *testing.T) {
	sc := NewShardedCache(8)
	for i := 0; i < 2000; i++ {
		sc.Put(fmt.Sprintf("key-%d", i), "v")
	}
	total := 0
	for i, s := range sc.shards {
		s.mu.RLock()
		n := len(s.data)
		s.mu.RUnlock()
		assert.Greater(t, n, 0, "shard %d empty: hash not spreading", i)
		total += n
	}
	assert.Equal(t, 2000, total)
}
