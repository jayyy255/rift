// Package routing maps record keys to partitions.
//
// The mapping is part of the compatibility contract: changing it would move
// existing keys to different partitions and break per-key ordering across an
// upgrade. Adaptive placement (milestone R6) changes which broker leads a
// partition, not which partition a key maps to.
package routing

import "hash/fnv"

// PartitionForKey returns the partition in [0, partitions) for key, using
// 32-bit FNV-1a. partitions must be positive.
func PartitionForKey(key []byte, partitions int32) int32 {
	h := fnv.New32a()
	h.Write(key)
	return int32(h.Sum32() % uint32(partitions))
}
