package sharding

import (
	"testing"
)

func TestCRC16Determinism(t *testing.T) {
	testCases := []string{
		"hello",
		"world",
		"",
		"a",
		"test_key_123",
		"こんにちは", // Unicode
		"very_long_key_" + string(make([]byte, 1000)),
	}

	for _, key := range testCases {
		hash1 := CRC16(key)
		hash2 := CRC16(key)
		hash3 := CRC16(key)

		if hash1 != hash2 || hash2 != hash3 {
			t.Errorf("CRC16 not deterministic for key %q: got %d, %d, %d", key, hash1, hash2, hash3)
		}
	}
}

func TestCRC16Uniqueness(t *testing.T) {
	// Test that different keys produce different hashes (mostly)
	// Note: Hash collisions are expected and acceptable for a 16-bit hash
	keys := []string{
		"key1",
		"key2",
		"key3",
		"different",
		"unique",
	}

	hashes := make(map[uint16]string)
	collisions := 0

	for _, key := range keys {
		hash := CRC16(key)
		if existingKey, exists := hashes[hash]; exists {
			t.Logf("Hash collision: %q and %q both hash to %d", key, existingKey, hash)
			collisions++
		}
		hashes[hash] = key
	}

	// We expect very few collisions in this small set
	if collisions > 1 {
		t.Errorf("Too many hash collisions: %d (expected <= 1)", collisions)
	}
}

func TestMurmurHash3Determinism(t *testing.T) {
	testCases := []string{
		"hello",
		"world",
		"",
		"a",
		"test_key_123",
		"こんにちは",
	}

	for _, key := range testCases {
		hash1 := MurmurHash3(key)
		hash2 := MurmurHash3(key)
		hash3 := MurmurHash3(key)

		if hash1 != hash2 || hash2 != hash3 {
			t.Errorf("MurmurHash3 not deterministic for key %q: got %d, %d, %d", key, hash1, hash2, hash3)
		}
	}
}

func TestHashFunctionsDifferent(t *testing.T) {
	// CRC16 and MurmurHash3 should produce different distributions
	testKeys := []string{
		"test1",
		"test2",
		"test3",
	}

	differences := 0
	for _, key := range testKeys {
		crc := CRC16(key)
		murmur := MurmurHash3(key)
		if crc != murmur {
			differences++
		}
	}

	// We expect most keys to hash differently between the two functions
	if differences == 0 {
		t.Error("CRC16 and MurmurHash3 produced identical results for all test keys")
	}
}

func TestHashEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{"empty string", ""},
		{"single char", "a"},
		{"unicode", "こんにちは世界"},
		{"emoji", "🚀🔥💡"},
		{"whitespace", "   "},
		{"newlines", "line1\nline2\nline3"},
		{"special chars", "!@#$%^&*()"},
		{"very long", string(make([]byte, 10000))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic
			hash1 := CRC16(tt.key)
			hash2 := MurmurHash3(tt.key)

			// Run again to verify determinism
			if hash1 != CRC16(tt.key) {
				t.Errorf("CRC16 not deterministic for %q", tt.name)
			}
			if hash2 != MurmurHash3(tt.key) {
				t.Errorf("MurmurHash3 not deterministic for %q", tt.name)
			}
		})
	}
}

func BenchmarkCRC16(b *testing.B) {
	keys := []string{
		"short",
		"medium_length_key_12345",
		"very_long_key_" + string(make([]byte, 1000)),
	}

	for _, key := range keys {
		b.Run(key, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = CRC16(key)
			}
		})
	}
}

func BenchmarkMurmurHash3(b *testing.B) {
	keys := []string{
		"short",
		"medium_length_key_12345",
		"very_long_key_" + string(make([]byte, 1000)),
	}

	for _, key := range keys {
		b.Run(key, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = MurmurHash3(key)
			}
		})
	}
}
