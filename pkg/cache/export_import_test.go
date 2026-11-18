package cache

import (
	"fmt"
	"hash/fnv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func shardFor(key string, total int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	s := int(h.Sum32()) % total
	if s < 0 {
		s = -s
	}
	return s
}

func TestExportShard_PartitionsKeySpaceExactly(t *testing.T) {
	c := NewConcurrentMapCache()
	for i := 0; i < 500; i++ {
		c.Put(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	for _, total := range []int{1, 2, 7, 16} {
		union := map[string]string{}
		for s := 0; s < total; s++ {
			part := c.ExportShard(s, total)
			for k, v := range part {
				assert.Equal(t, s, shardFor(k, total), "key %s exported by wrong shard", k)
				_, dup := union[k]
				assert.False(t, dup, "key %s exported by two shards (total=%d)", k, total)
				union[k] = v
			}
		}
		assert.Len(t, union, 500, "every key belongs to exactly one shard (total=%d)", total)
	}
}

func TestExportShard_WrongShardAndEmpty(t *testing.T) {
	c := NewConcurrentMapCache()
	assert.Empty(t, c.ExportShard(0, 4), "empty cache")
	c.Put("k", "v")
	assert.Empty(t, c.ExportShard(99, 4), "shard id beyond range exports nothing")
	assert.Empty(t, c.ExportShard(-1, 4), "negative shard exports nothing")
	assert.NotNil(t, c.ExportShard(99, 4), "result is never nil")
}

func TestExportShard_NonPositiveTotalShardsDoesNotPanic(t *testing.T) {
	c := NewConcurrentMapCache()
	c.Put("k", "v")
	assert.NotPanics(t, func() { assert.Empty(t, c.ExportShard(0, 0)) })
	assert.NotPanics(t, func() { assert.Empty(t, c.ExportShard(0, -3)) })
}

func TestExportShard_ReturnsIndependentCopy(t *testing.T) {
	c := NewConcurrentMapCache()
	c.Put("k", "v")
	out := c.ExportShard(0, 1)
	out["k"] = "tampered"
	out["new"] = "x"
	assert.Equal(t, "v", c.Get("k"))
	assert.Equal(t, "", c.Get("new"))
}

func TestImport_ConflictsOverwriteAndNilIsNoop(t *testing.T) {
	c := NewConcurrentMapCache()
	c.Put("a", "local")
	c.Put("only-local", "keep")

	c.Import(nil)
	c.Import(map[string]string{})
	assert.Equal(t, "local", c.Get("a"))

	c.Import(map[string]string{"a": "imported", "b": "new"})
	assert.Equal(t, "imported", c.Get("a"), "import is last-writer-wins")
	assert.Equal(t, "new", c.Get("b"))
	assert.Equal(t, "keep", c.Get("only-local"), "import never deletes")
}

func TestImport_DoesNotAliasCallerMap(t *testing.T) {
	c := NewConcurrentMapCache()
	in := map[string]string{"k": "v"}
	c.Import(in)
	in["k"] = "mutated"
	assert.Equal(t, "v", c.Get("k"))
}

func TestExportImport_RoundTripBetweenCaches(t *testing.T) {
	src, dst := NewConcurrentMapCache(), NewConcurrentMapCache()
	for i := 0; i < 300; i++ {
		src.Put(fmt.Sprintf("k%d", i), "v")
	}
	dst.Import(src.ExportShard(3, 5))
	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("k%d", i)
		if shardFor(k, 5) == 3 {
			require.Equal(t, "v", dst.Get(k))
		} else {
			require.Equal(t, "", dst.Get(k))
		}
	}
}

func TestExportImport_ConcurrentWithWriters(t *testing.T) {
	c := NewConcurrentMapCache()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				c.Put(fmt.Sprintf("w%d-%d", w, i), "v")
				if i%5 == 0 {
					c.Delete(fmt.Sprintf("w%d-%d", w, i))
				}
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				data := c.ExportShard(r, 4)
				c.Import(data) // re-importing a snapshot is the catch-up path
			}
		}(r)
	}
	wg.Wait()
}
