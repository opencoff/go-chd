// marshal.go - Marshal/Unmarshal for CHD datastructure
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
	"fmt"
	"io"
	//"encoding/binary"
)

const _ChdHeaderSize = 8 // 4 x 64-bit words

// MarshalBinary encodes the hash into a binary form suitable for durable storage.
// A subsequent call to UnmarshalBinary() will reconstruct the CHD instance.
func (c *Chd) MarshalBinary(w io.Writer) (int, error) {
	// Header: 1 64-bit words:
	//   o version byte
	//   o resv [7]byte
	//
	// Body:
	//   o <n> seeds laid out sequentially

	var x [_ChdHeaderSize]byte // 4 x 64-bit words

	x[0] = 1
	nw, err := writeAll(w, x[:])
	if err != nil {
		return 0, err
	}

	// Instead of writing one seed at a time, we re-interpret
	// c.seeds as a byte-slice and write it out.
	bs := u32sToByteSlice(c.seeds)
	n, err := writeAll(w, bs)
	if err != nil {
		return nw, err
	}

	return n + nw, nil
}

// UnmarshalBinaryMmap reads a previously marshalled Chd instance and returns
// a lookup table. It assumes that buf is memory-mapped and aligned at the
// right boundaries.
func (c *Chd) UnmarshalBinaryMmap(buf []byte) error {
	hdr := buf[:_ChdHeaderSize]
	if hdr[0] != 1 {
		return fmt.Errorf("chd: no support to un-marshal version %d", hdr[0])
	}

	c.seeds = bsToUint32Slice(buf[_ChdHeaderSize:])
	return nil
}
