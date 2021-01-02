// mmap.go -- mmap a slice of ints/uints from a file
//
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
	"reflect"
	"unsafe"
)

// byte-slice to uint64 slice
func bsToUint64Slice(b []byte) []uint64 {
	n := len(b) / 8
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	var v []uint64

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&v))
	sh.Data = bh.Data
	sh.Len = n
	sh.Cap = n

	return v
}

// byte-slice to uint32 slice
func bsToUint32Slice(b []byte) []uint32 {
	n := len(b) / 4
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	var v []uint32

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&v))
	sh.Data = bh.Data
	sh.Len = n
	sh.Cap = n

	return v
}

// uint32 slice to byte-slice
func u32sToByteSlice(b []uint32) []byte {
	n := len(b)
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	var v []byte

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&v))
	sh.Data = bh.Data
	sh.Len = n * 4
	sh.Cap = n * 4

	return v
}

// uint64 slice to byte-slice
func u64sToByteSlice(b []uint64) []byte {
	n := len(b)
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	var v []byte

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&v))
	sh.Data = bh.Data
	sh.Len = n * 8
	sh.Cap = n * 8

	return v
}
