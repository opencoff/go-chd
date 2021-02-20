// rand.go -- utilities that generate random values
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
	"crypto/rand"
	"encoding/binary"
	"io"
)

func randbytes(n int) []byte {
	b := make([]byte, n)

	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		panic("can't read crypto/rand")
	}
	return b
}

func rand32() uint32 {
	var b [4]byte

	_, err := io.ReadFull(rand.Reader, b[:])
	if err != nil {
		panic("can't read crypto/rand")
	}

	return binary.BigEndian.Uint32(b[:])
}

func rand64() uint64 {
	var b [8]byte

	_, err := io.ReadFull(rand.Reader, b[:])
	if err != nil {
		panic("can't read crypto/rand")
	}

	return binary.BigEndian.Uint64(b[:])
}
