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
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

const (
	// number of times we will try to build the table
	_MaxSeed uint32 = 65536 * 2
)

// ChdBuilder is used to create a MPHF from a given set of uint64 keys
type ChdBuilder struct {
	data map[uint64]bool
	salt uint64
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
		salt: rand64(),
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
	seeds := make([]uint32, m)

	for i := range buckets {
		b := &buckets[i]
		b.slot = uint64(i)
	}

	for key, _ := range c.data {
		j := rhash(0, key, m, c.salt)
		b := &buckets[j]
		b.keys = append(b.keys, key)
	}

	occ := newBitVector(m)
	bOcc := newBitVector(m)

	// sort buckets in decreasing order of occupancy-size
	sort.Sort(buckets)

	tries := 0
	var maxseed uint32
	for i := range buckets {
		b := &buckets[i]
		for s := uint32(1); s < _MaxSeed; s++ {
			bOcc.Reset()
			for _, key := range b.keys {
				h := rhash(s, key, m, c.salt)
				if occ.IsSet(h) || bOcc.IsSet(h) {
					goto nextSeed // try next seed
				}
				bOcc.Set(h)
			}
			occ.Merge(bOcc)
			seeds[b.slot] = s
			if s > maxseed {
				maxseed = s
			}
			goto nextBucket

		nextSeed:
			tries++
		}

		return nil, fmt.Errorf("chd: No MPH after %d tries", _MaxSeed)
	nextBucket:
	}

	chd := &Chd{
		seed:  makeSeeds(seeds, maxseed),
		salt:  c.salt,
		tries: tries,
	}

	return chd, nil
}

func makeSeeds(s []uint32, max uint32) seeder {
	switch {
	case max < 256:
		return newU8(s)

	case max < 65536:
		return newU16(s)

	default:
		return newU32(s)
	}
}

// Chd represents a frozen PHF for the given set of keys
type Chd struct {
	seed  seeder
	salt  uint64
	tries int
}

func (c *Chd) SeedSize() byte {
	return c.seed.seedsize()
}

// Len returns the actual length of the PHF lookup table
func (c *Chd) Len() int {
	return c.seed.length()
}

// Find returns a unique integer representing the minimal hash for key 'k'.
// The return value is meaningful ONLY for keys in the original key set (provided
// at the time of construction of the minimal-hash).
// Callers should verify that the key at the returned index == k.
func (c *Chd) Find(k uint64) uint64 {
	m := uint64(c.seed.length())
	h := rhash(0, k, m, c.salt)
	return rhash(c.seed.seed(h), k, m, c.salt)
}

// CHD Marshalled header - 2 x 64-bit words
const _ChdHeaderSize = 16

// To compress the seed table, we will use the interface below to abstract
// seed table of different sizes: 1, 2, 4
type seeder interface {
	// given a hash index, return the seed at the index
	seed(uint64) uint32

	// marshal to writer 'w'
	marshal(w io.Writer) (int, error)

	// unmarshal from mem-mapped byte slice 'b'
	unmarshal(b []byte) error

	// size of each seed in bytes (1, 2, 4)
	seedsize() byte

	// # of seeds
	length() int
}

// ensure each of these types implement the seeder interface above.
var (
	_ seeder = &u8Seeder{}
	_ seeder = &u16Seeder{}
	_ seeder = &u32Seeder{}
)

// 8 bit seed
type u8Seeder struct {
	seeds []uint8
}

func newU8(v []uint32) seeder {
	bs := make([]byte, len(v))
	for i, a := range v {
		bs[i] = byte(a & 0xff)
	}

	s := &u8Seeder{
		seeds: bs,
	}
	return s
}

func (u *u8Seeder) seed(v uint64) uint32 {
	return uint32(u.seeds[v])
}

func (u *u8Seeder) length() int {
	return len(u.seeds)
}

func (u *u8Seeder) seedsize() byte {
	return 1
}

func (u *u8Seeder) marshal(w io.Writer) (int, error) {
	return writeAll(w, u.seeds)
}

func (u *u8Seeder) unmarshal(b []byte) error {
	u.seeds = b
	return nil
}

// 16 bit seed
type u16Seeder struct {
	seeds []uint16
}

func newU16(v []uint32) seeder {
	us := make([]uint16, len(v))
	for i, a := range v {
		us[i] = uint16(a & 0xffff)
	}

	s := &u16Seeder{
		seeds: us,
	}
	return s
}

func (u *u16Seeder) seed(v uint64) uint32 {
	return uint32(u.seeds[v])
}

func (u *u16Seeder) length() int {
	return len(u.seeds)
}
func (u *u16Seeder) seedsize() byte {
	return 2
}

func (u *u16Seeder) marshal(w io.Writer) (int, error) {
	bs := u16sToByteSlice(u.seeds)
	return writeAll(w, bs)
}

func (u *u16Seeder) unmarshal(b []byte) error {
	u.seeds = bsToUint16Slice(b)
	return nil
}

// 32 bit seed
type u32Seeder struct {
	seeds []uint32
}

func newU32(v []uint32) seeder {
	s := &u32Seeder{
		seeds: v,
	}
	return s
}

func (u *u32Seeder) seed(v uint64) uint32 {
	return uint32(u.seeds[v])
}

func (u *u32Seeder) length() int {
	return len(u.seeds)
}

func (u *u32Seeder) seedsize() byte {
	return 4
}

func (u *u32Seeder) marshal(w io.Writer) (int, error) {
	bs := u32sToByteSlice(u.seeds)
	return writeAll(w, bs)
}

func (u *u32Seeder) unmarshal(b []byte) error {
	u.seeds = bsToUint32Slice(b)
	return nil
}

// MarshalBinary encodes the hash into a binary form suitable for durable storage.
// A subsequent call to UnmarshalBinary() will reconstruct the CHD instance.
func (c *Chd) MarshalBinary(w io.Writer) (int, error) {
	// Header: 2 64-bit words:
	//   o version byte
	//   o CHD_Seed_Size byte
	//   o resv [6]byte
	//   o salt 8 bytes
	//
	// Body:
	//   o <n> seeds laid out sequentially

	var x [_ChdHeaderSize]byte // 4 x 64-bit words

	x[0] = 1
	x[1] = c.SeedSize()
	binary.LittleEndian.PutUint64(x[8:], c.salt)
	nw, err := writeAll(w, x[:])
	if err != nil {
		return 0, err
	}

	m, err := c.seed.marshal(w)
	return nw + m, err
}

// Dump CHD meta-data to io.Writer 'w'
func (c *Chd) DumpMeta(w io.Writer) {
	switch c.seed.(type) {
	case *u8Seeder:
		fmt.Fprintf(w, "  CHD with 8-bit seeds <salt %#x>\n", c.salt)
	case *u16Seeder:
		fmt.Fprintf(w, "  CHD with 16-bit seeds <salt %#x>\n", c.salt)
	case *u32Seeder:
		fmt.Fprintf(w, "  CHD with 32-bit seeds <salt %#x>\n", c.salt)

	default:
		panic("Unknown seed type!")
	}
}

// UnmarshalBinaryMmap reads a previously marshalled Chd instance and returns
// a lookup table. It assumes that buf is memory-mapped and aligned at the
// right boundaries.
func (c *Chd) UnmarshalBinaryMmap(buf []byte) error {
	hdr := buf[:_ChdHeaderSize]
	if hdr[0] != 1 {
		return fmt.Errorf("chd: no support to un-marshal version %d", hdr[0])
	}

	var seed seeder

	size := hdr[1]
	salt := binary.LittleEndian.Uint64(hdr[8:])
	vals := buf[_ChdHeaderSize:]

	switch size {
	case 1:
		u8 := &u8Seeder{}
		if err := u8.unmarshal(vals); err != nil {
			return nil
		}
		seed = u8
	case 2:
		if (len(vals) % 2) != 0 {
			return fmt.Errorf("chd: partial seeds of size 2 (exp %d, saw %d)",
				len(vals)+1, len(vals))
		}

		u16 := &u16Seeder{}
		if err := u16.unmarshal(vals); err != nil {
			return err
		}
		seed = u16

	case 4:
		if (len(vals) % 4) != 0 {
			return fmt.Errorf("chd: partial seeds of size 2 (exp %d, saw %d)",
				len(vals)+3/4, len(vals))
		}
		u32 := &u32Seeder{}
		if err := u32.unmarshal(vals); err != nil {
			return err
		}
		seed = u32

	default:
		return fmt.Errorf("chd: unknown seed-size %d", size)
	}

	c.seed = seed
	c.salt = salt
	return nil
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
func rhash(seed uint32, key, sz, salt uint64) uint64 {
	const m uint64 = 0x880355f21e6d1965
	var h uint64 = key

	h *= m
	h ^= mix(salt)
	h *= m
	h ^= mix(uint64(seed))
	h *= m
	return mix(h) & (sz - 1)
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
