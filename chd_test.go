// chd_test.go -- test suite for chd
//
// (c) Sudhi Herle 2018
//
// Author: Sudhi Herle <sudhi@herle.net>
//
// This software does not come with any express or implied
// warranty; it is provided "as is". No claim  is made to its
// suitability for any purpose.

package chd

import (
	"bytes"
	"testing"

	"github.com/opencoff/go-fasthash"
)

var keyw = []string{
	"expectoration",
	"mizzenmastman",
	"stockfather",
	"pictorialness",
	"villainous",
	"unquality",
	"sized",
	"Tarahumari",
	"endocrinotherapy",
	"quicksandy",
	"heretics",
	"pediment",
	"spleen's",
	"Shepard's",
	"paralyzed",
	"megahertzes",
	"Richardson's",
	"mechanics's",
	"Springfield",
	"burlesques",
}

func TestCHDSimple(t *testing.T) {
	assert := newAsserter(t)

	c, err := New()
	assert(err == nil, "construction failed: %s", err)
	kvmap := make(map[uint64]string) // map of hash to string
	kmap := make(map[uint64]uint64)  // map of index to hashval

	hseed := rand64()
	for _, s := range keyw {
		h := fasthash.Hash64(hseed, []byte(s))
		kvmap[h] = s
		c.Add(h)
	}

	lookup, err := c.Freeze(0.9)
	assert(err == nil, "freeze: %s", err)
	nkeys := uint64(lookup.Len())

	for h, s := range kvmap {
		j := lookup.Find(h)
		assert(j <= nkeys, "key %s <%#x> mapping %d out-of-bounds", s, h, j)

		x, ok := kmap[j]
		assert(!ok, "index %d already mapped to key %#x", j, x)

		//t.Logf("key %x -> %d\n", h, j)
		kmap[j] = h
	}
}

func TestCHDMarshal(t *testing.T) {
	assert := newAsserter(t)

	b, err := New()
	assert(err == nil, "construction failed: %s", err)

	hseed := rand64()
	keys := make([]uint64, len(keyw))
	for i, s := range keyw {
		keys[i] = fasthash.Hash64(hseed, []byte(s))
		b.Add(keys[i])
	}

	c, err := b.Freeze(0.9)
	assert(err == nil, "freeze failed: %s", err)

	var buf bytes.Buffer

	n, err := c.MarshalBinary(&buf)
	assert(err == nil, "marshal failed: %s", err)

	t.Logf("marshal size: %d bytes\n", n)

	var c2 Chd
	err = c2.UnmarshalBinaryMmap(buf.Bytes())
	assert(err == nil, "unmarshal failed: %s", err)

	for i, k := range keys {
		x := c.Find(k)
		y := c2.Find(k)
		assert(x == y, "b and b2 mapped key %d <%#x>: %d vs. %d", i, k, x, y)
	}
}
