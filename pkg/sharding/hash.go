package sharding

import (
	"hash/crc32"
)

// HashFunc is a function type that computes a hash value for a given key
type HashFunc func(key string) uint16

// CRC16 implements the CRC16 algorithm used by Redis Cluster
// This provides good distribution and is fast to compute
func CRC16(key string) uint16 {
	// Use CRC32 as Go standard library doesn't have CRC16
	// Then reduce to 16 bits with XOR folding for good distribution
	crc := crc32.ChecksumIEEE([]byte(key))

	// XOR-fold 32 bits into 16 bits
	// This maintains good distribution properties
	return uint16((crc >> 16) ^ (crc & 0xFFFF))
}

// MurmurHash3 provides an alternative hash function with excellent distribution
// This is a simplified version - production code may want a full MurmurHash3 implementation
func MurmurHash3(key string) uint16 {
	const (
		c1 = 0xcc9e2d51
		c2 = 0x1b873593
		r1 = 15
		r2 = 13
		m  = 5
		n  = 0xe6546b64
	)

	hash := uint32(0)
	data := []byte(key)

	// Process 4-byte chunks
	nblocks := len(data) / 4
	for i := 0; i < nblocks; i++ {
		k := uint32(data[i*4]) | uint32(data[i*4+1])<<8 |
		     uint32(data[i*4+2])<<16 | uint32(data[i*4+3])<<24

		k *= c1
		k = (k << r1) | (k >> (32 - r1))
		k *= c2

		hash ^= k
		hash = (hash << r2) | (hash >> (32 - r2))
		hash = hash*m + n
	}

	// Process remaining bytes
	tail := data[nblocks*4:]
	k := uint32(0)
	switch len(tail) {
	case 3:
		k ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k ^= uint32(tail[0])
		k *= c1
		k = (k << r1) | (k >> (32 - r1))
		k *= c2
		hash ^= k
	}

	// Finalization
	hash ^= uint32(len(data))
	hash ^= hash >> 16
	hash *= 0x85ebca6b
	hash ^= hash >> 13
	hash *= 0xc2b2ae35
	hash ^= hash >> 16

	// Fold to 16 bits
	return uint16((hash >> 16) ^ (hash & 0xFFFF))
}

// DefaultHashFunc is the default hash function used by the partitioner
var DefaultHashFunc = CRC16
