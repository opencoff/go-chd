// bitvector.go -- sets out of bitvectors
//
// (c) Sudhi Herle 2018
//
// Author: Sudhi Herle <sudhi@herle.net>
//
// This software does not come with any express or implied
// warranty; it is provided "as is". No claim  is made to its
// suitability for any purpose.

package chd

// bitVector represents a bit vector in an efficient manner
type bitVector struct {
	v []uint64

}

// newbitVector creates a bitvector to hold atleast 'size' bits.
func newBitVector(size uint64) *bitVector {
	sz := size + 63
	sz &= ^uint64(63)
	words := sz / 64
	bv := &bitVector{
		v: make([]uint64, words),
	}

	return bv
}

// Size returns the number of bits in this bitvector
func (b *bitVector) Size() uint64 {
	return uint64(len(b.v) * 64)
}

// Words returns the number of words in the array
func (b *bitVector) Words() uint64 {
	return uint64(len(b.v))
}

// Set sets the bit 'i' in the bitvector
func (b *bitVector) Set(i uint64) *bitVector {
	pv := &b.v[i/64]
	*pv |=  uint64(1) << (i % 64)
	return b
}


// Clear clears bit 'i'
func (b *bitVector) Clear(i uint64) *bitVector {
	pv := &b.v[i/64]
	*pv &= ^(uint64(1) << (i % 64))
	return b
}

// IsSet() returns true if the bit 'i' is set, false otherwise
func (b *bitVector) IsSet(i uint64) bool {
	w := b.v[i/64]
	return 1 == (1 & (w >> (i % 64)))
}

// Reset() clears all the bits in the bitvector
func (b *bitVector) Reset() *bitVector {
	v := b.v
	for i := range v {
		v[i] = 0
	}
	return b
}

// merge new bitvector 'x' into 'b'
func (b *bitVector) Merge(x *bitVector) *bitVector {
	v := b.v
	for i, z := range x.v {
		v[i] |= z
	}
	return b
}

