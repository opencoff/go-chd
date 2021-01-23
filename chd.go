// chd.go - fast minimal perfect hashing for massive key sets
//
// This is an implementation of CHD in http://cmph.sourceforge.net/papers/esa09.pdf -
// inspired by this https://gist.github.com/pervognsen/b21f6dd13f4bcb4ff2123f0d78fcfd17
//
// (c) Sudhi Herle 2018
//
// License GPLv2

// Package chd implements ChdBuilder - to create fast, minimal perfect hash functions from
// a given set of keys. This is an implementation of CHD in
// http://cmph.sourceforge.net/papers/esa09.pdf -
//
// Additionally, DBWriter enables creating a fast, constant-time DB for read-only workloads.
// It serializes the key,value pairs and builds a CHD minimal perfect hash function over the
// given keys. The serialized DB can be read back via DBReader for constant time lookups
// of the MPH DB.
package chd

import (
	"fmt"
	"sort"
)

const (
	// number of times we will try to build the table
	_MaxSeed uint64 = 1000000
)

// ChdBuilder is used to create a MPHF from a given set of uint64 keys
type ChdBuilder struct {
	data map[uint64]bool
}

// New enables creation of a minimal perfect hash function via the
// Compress Hash Displace algorithm. Once created, callers can
// add keys to it before Freezing the MPH and generating a constant time
// lookup table. This implementation of CHD uses uint64 keys. Callers
// can use any good hash function (murmur hash etc.) to map their data into
// these keys.
// Once the construction is frozen, callers can use "Find()" to find the
// unique mapping for each key in 'keys'.
func New() (*ChdBuilder, error) {
	c := &ChdBuilder{
		data: make(map[uint64]bool),
	}

	return c, nil
}

// Add a new key to the MPH builder
func (c *ChdBuilder) Add(key uint64) error {
	if _, ok := c.data[key]; ok {
		return fmt.Errorf("chd: duplicate key %x", key)
	}

	c.data[key] = true
	return nil
}

type bucket struct {
	slot uint64
	keys []uint64
}
type buckets []bucket

func (b buckets) Len() int {
	return len(b)
}

func (b buckets) Less(i, j int) bool {
	return len(b[i].keys) > len(b[j].keys)
}

func (b buckets) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

// Freeze builds a constant-time lookup table using the CMD algorithm and
// the given load factor. Lower load factors speeds up the construction
// of the MPHF. Suggested value for load is between 0.75-0.9
func (c *ChdBuilder) Freeze(load float64) (*Chd, error) {
	if load < 0 || load > 1 {
		return nil, fmt.Errorf("chd: invalid load factor %f", load)
	}

	m := uint64(float64(len(c.data)) / load)
	m = nextpow2(m)
	buckets := make(buckets, m)
	seeds := make([]uint64, m)

	for key, _ := range c.data {
		j := rhash(0, key, m)
		b := &buckets[j]
		b.slot = j // original slot assigned to this key
		b.keys = append(b.keys, key)
	}

	occ := newBitVector(m)
	bOcc := newBitVector(m)

	// sort buckets in decreasing order of occupancy-size
	sort.Sort(buckets)

	tries := 0
	for i := range buckets {
		b := &buckets[i]
		for s := uint64(1); s < _MaxSeed; s++ {
			bOcc.Reset()
			for _, key := range b.keys {
				h := rhash(s, key, m)
				if occ.IsSet(h) || bOcc.IsSet(h) {
					goto nextSeed // try next seed
				}
				bOcc.Set(h)
			}
			occ.Merge(bOcc)
			seeds[b.slot] = s
			goto nextBucket

		nextSeed:
			tries++
		}

		return nil, fmt.Errorf("chd: No MPH after %d tries", _MaxSeed)
	nextBucket:
	}

	chd := &Chd{
		seeds: seeds,
		tries: tries,
	}
	return chd, nil
}

// Chd represents a frozen PHF for the given set of keys
type Chd struct {
	seeds []uint64
	tries int
}

// Len returns the actual length of the PHF lookup table
func (c *Chd) Len() int {
	return len(c.seeds)
}

// Find returns a unique integer representing the minimal hash for key 'k'.
// The return value is meaningful ONLY for keys in the original key set (provided
// at the time of construction of the minimal-hash).
// Callers should verify that the key at the returned index == k.
func (c *Chd) Find(k uint64) uint64 {
	m := uint64(len(c.seeds))
	h := rhash(0, k, m)
	return rhash(c.seeds[h], k, m)
}

// compression function for fasthash
// borrowed from Zi Long Tan's superfast hash
func mix(h uint64) uint64 {
	h ^= h >> 23
	h *= 0x2127599bf4325c37
	h ^= h >> 47
	return h
}

// hash key with a given seed and return the result modulo 'sz'.
// 'sz' is guarantted to be a power of 2; so, modulo can be fast.
// borrowed from Zi Long Tan's superfast hash
func rhash(seed uint64, key uint64, sz uint64) uint64 {
	const m uint64 = 0x880355f21e6d1965
	var h uint64 = key + ^seed

	h ^= mix(h)
	h *= m
	return h & (sz - 1)
}

// return next power of 2
func nextpow2(n uint64) uint64 {
	n = n - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	return n + 1
}
