package shardlet

import "hash/fnv"

func hashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

func shardFor(key string, shardCount int) int {
	return int(hashKey(key) % uint64(shardCount))
}
