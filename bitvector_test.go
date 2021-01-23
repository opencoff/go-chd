// bitvector_test.go -- test suite for bitvector
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
	"testing"
)

func TestBitVectorSimple(t *testing.T) {
	assert := newAsserter(t)

	bv := newBitVector(100)
	assert(bv.Size() == 128, "size mismatch; exp 128, saw %d", bv.Size())

	for i := uint64(0); i < bv.Size(); i++ {
		if 1 == (i & 1) {
			bv.Set(i)
		}
	}

	for i := uint64(0); i < bv.Size(); i++ {
		if 1 == (i & 1) {
			assert(bv.IsSet(i), "%d not set", i)
		} else {
			assert(!bv.IsSet(i), "%d is set", i)
		}
	}
}

func TestBitVectorMerge(t *testing.T) {
	assert := newAsserter(t)

	av := newBitVector(60)
	bv := newBitVector(60)
	assert(av.Size() == 64, "a:size mismatch; exp 64, saw %d", av.Size())
	assert(bv.Size() == 64, "b:size mismatch; exp 64, saw %d", bv.Size())

	for i := uint64(0); i < av.Size(); i++ {
		if 1 == (i & 1) {
			bv.Set(i)
		} else {
			av.Set(i)
		}
	}

	av.Merge(bv)
	for i := uint64(0); i < av.Size(); i++ {
		assert(av.IsSet(i), "merged bit %d not set", i)
	}

}
