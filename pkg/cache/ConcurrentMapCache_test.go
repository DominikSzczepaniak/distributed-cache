package cache

import (
	"hash/fnv"
	"testing"
)

func TestConcurrentMapCache_ExportShard(t *testing.T) {
	c := NewConcurrentMapCache()
	totalShards := 10
	targetShard := 1

	getShard := func(key string) int {
		h := fnv.New32a()
		h.Write([]byte(key))
		s := int(h.Sum32()) % totalShards
		if s < 0 {
			s = -s
		}
		return s
	}

	expectedKeys := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := string(rune(i))
		val := "val"
		c.Put(key, val)

		if getShard(key) == targetShard {
			expectedKeys[key] = val
		}
	}

	exported := c.ExportShard(targetShard, totalShards)

	if len(exported) != len(expectedKeys) {
		t.Errorf("Expected %d keys, got %d", len(expectedKeys), len(exported))
	}

	for k, v := range expectedKeys {
		if gotV, ok := exported[k]; !ok || gotV != v {
			t.Errorf("Missing or incorrect key %s: expected %s, got %s", k, v, gotV)
		}
	}

	for k := range exported {
		if _, ok := expectedKeys[k]; !ok {
			t.Errorf("Exported key %s that shouldn't be there (shard %d)", k, getShard(k))
		}
	}
}

func TestConcurrentMapCache_Import(t *testing.T) {
	c := NewConcurrentMapCache()
	data := map[string]string{
		"k1": "v1",
		"k2": "v2",
	}

	c.Import(data)

	if c.Get("k1") != "v1" {
		t.Errorf("Import failed for k1")
	}
	if c.Get("k2") != "v2" {
		t.Errorf("Import failed for k2")
	}
}
