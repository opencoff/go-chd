// dbreader.go -- Constant DB built on top of the CHD MPH
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
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"syscall"

	"crypto/sha512"
	"crypto/subtle"

	"github.com/dchest/siphash"
	"github.com/opencoff/golang-lru"
)

// DBReader represents the query interface for a previously constructed
// constant database (built using NewDBWriter()). The only meaningful
// operation on such a database is Lookup().
type DBReader struct {
	chd *Chd

	cache *lru.ARCCache

	// memory mapped offset+hashkey table
	offset []uint64

	// memory mapped vlen table
	vlen []uint32

	nkeys uint64
	salt  []byte

	// original mmap slice
	mmap []byte
	fd   *os.File
	fn   string
}

// NewDBReader reads a previously construct database in file 'fn' and prepares
// it for querying. Records are opportunistically cached after reading from disk.
// We retain upto 'cache' number of records in memory (default 128).
func NewDBReader(fn string, cache int) (rd *DBReader, err error) {
	fd, err := os.Open(fn)
	if err != nil {
		return nil, err
	}

	// Number of records to cache
	if cache <= 0 {
		cache = 128
	}

	rd = &DBReader{
		chd:  &Chd{},
		salt: make([]byte, 16),
		fd:   fd,
		fn:   fn,
	}

	var st os.FileInfo

	st, err = fd.Stat()
	if err != nil {
		return nil, fmt.Errorf("%s: can't stat: %s", fn, err)
	}

	if st.Size() < (64 + 32) {
		return nil, fmt.Errorf("%s: file too small or corrupted", fn)
	}

	var hdrb [64]byte

	_, err = io.ReadFull(fd, hdrb[:])
	if err != nil {
		return nil, fmt.Errorf("%s: can't read header: %s", fn, err)
	}

	offtbl, err := rd.decodeHeader(hdrb[:], st.Size())
	if err != nil {
		return nil, err
	}

	err = rd.verifyChecksum(hdrb[:], offtbl, st.Size())
	if err != nil {
		return nil, err
	}

	// All metadata is now verified.
	// sanity check - even though we have verified the strong checksum
	// 8 + 8 + 4: offset, hashkey, vlen
	tblsz := rd.nkeys * (8 + 8 + 4)

	// 64 + 32: 64 bytes of header, 32 bytes of sha trailer
	if uint64(st.Size()) < (64 + 32 + tblsz) {
		return nil, fmt.Errorf("%s: corrupt header", fn)
	}

	rd.cache, err = lru.NewARC(cache)
	if err != nil {
		return nil, err
	}

	// Now, we are certain that the header, the offset-table and chd bits are
	// all valid and uncorrupted.

	// mmap the offset table
	mmapsz := st.Size() - int64(offtbl) - 32
	bs, err := syscall.Mmap(int(fd.Fd()), int64(offtbl), int(mmapsz), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, fmt.Errorf("%s: can't mmap %d bytes at off %d: %s",
			fn, mmapsz, offtbl, err)
	}

	offsz := rd.nkeys * (8 + 8)
	vlensz := rd.nkeys * 4

	rd.mmap = bs
	rd.offset = bsToUint64Slice(bs[:offsz])
	rd.vlen = bsToUint32Slice(bs[offsz : offsz+vlensz])

	// The CHD table starts here
	if err := rd.chd.UnmarshalBinaryMmap(bs[offsz+vlensz:]); err != nil {
		return nil, fmt.Errorf("%s: can't unmarshal hash table: %s", fn, err)
	}

	return rd, nil
}

// TotalKeys returns the total number of distinct keys in the DB
func (rd *DBReader) Len() int {
	return int(rd.nkeys)
}

// Close closes the db
func (rd *DBReader) Close() {
	syscall.Munmap(rd.mmap)
	rd.fd.Close()
	rd.cache.Purge()
	rd.chd = nil
	rd.fd = nil
	rd.salt = nil
	rd.fn = ""
}

// Lookup looks up 'key' in the table and returns the corresponding value.
// If the key is not found, value is nil and returns false.
func (rd *DBReader) Lookup(key uint64) ([]byte, bool) {
	v, err := rd.Find(key)
	if err != nil {
		return nil, false
	}

	return v, true
}

// Find looks up 'key' in the table and returns the corresponding value.
// It returns an error if the key is not found or the disk i/o failed or
// the record checksum failed.
func (rd *DBReader) Find(key uint64) ([]byte, error) {
	if v, ok := rd.cache.Get(key); ok {
		return v.([]byte), nil
	}

	// Not in cache. So, go to disk and find it.
	// We are guaranteed that: 0 <= i < rd.nkeys
	i := rd.chd.Find(key)
	j := i * 2
	if hash := toLittleEndianUint64(rd.offset[j+1]); hash != key {
		return nil, ErrNoKey
	}

	vlen := toLittleEndianUint32(rd.vlen[i])
	off := toLittleEndianUint64(rd.offset[j])

	val, err := rd.decodeRecord(off, vlen)
	if err != nil {
		return nil, err
	}

	rd.cache.Add(key, val)
	return val, nil
}

// read the next full record at offset 'off' - by seeking to that offset.
// calculate the record checksum, validate it and so on.
func (rd *DBReader) decodeRecord(off uint64, vlen uint32) ([]byte, error) {
	_, err := rd.fd.Seek(int64(off), 0)
	if err != nil {
		return nil, err
	}

	data := make([]byte, vlen+8)

	_, err = io.ReadFull(rd.fd, data)
	if err != nil {
		return nil, err
	}

	be := binary.BigEndian
	csum := be.Uint64(data[:8])

	var o [8]byte

	be.PutUint64(o[:], off)

	h := siphash.New(rd.salt)
	h.Write(o[:])
	h.Write(data[8:])
	exp := h.Sum64()

	if csum != exp {
		return nil, fmt.Errorf("%s: corrupted record at off %d (exp %#x, saw %#x)", rd.fn, off, exp, csum)
	}
	return data[8:], nil
}

// Verify checksum of all metadata: offset table, chd bits and the file header.
// We know that offtbl is within the size bounds of the file - see decodeHeader() below.
// sz is the actual file size (includes the header we already read)
func (rd *DBReader) verifyChecksum(hdrb []byte, offtbl uint64, sz int64) error {
	h := sha512.New512_256()
	h.Write(hdrb[:])

	// remsz is the size of the remaining metadata (which begins at offset 'offtbl')
	// 32 bytes of SHA512_256 and the values already recorded.
	remsz := sz - int64(offtbl) - 32

	rd.fd.Seek(int64(offtbl), 0)

	nw, err := io.CopyN(h, rd.fd, remsz)
	if err != nil {
		return fmt.Errorf("%s: metadata i/o error: %s", rd.fn, err)
	}
	if nw != remsz {
		return fmt.Errorf("%s: partial read while verifying checksum, exp %d, saw %d", rd.fn, remsz, nw)
	}

	var expsum [32]byte

	// Read the trailer -- which is the expected checksum
	rd.fd.Seek(sz-32, 0)
	_, err = io.ReadFull(rd.fd, expsum[:])
	if err != nil {
		return fmt.Errorf("%s: checksum i/o error: %s", rd.fn, err)
	}

	csum := h.Sum(nil)
	if subtle.ConstantTimeCompare(csum[:], expsum[:]) != 1 {
		return fmt.Errorf("%s: checksum failure; exp %#x, saw %#x", rd.fn, expsum[:], csum[:])
	}

	rd.fd.Seek(int64(offtbl), 0)
	return nil
}

// entry condition: b is 64 bytes long.
func (rd *DBReader) decodeHeader(b []byte, sz int64) (uint64, error) {
	if string(b[:4]) != "CHDB" {
		return 0, fmt.Errorf("%s: bad file magic", rd.fn)
	}

	be := binary.BigEndian
	i := 8 // skip the magic and flags

	rd.salt = b[i : i+16]
	i += 16
	rd.nkeys = be.Uint64(b[i : i+8])
	i += 8
	offtbl := be.Uint64(b[i : i+8])

	if offtbl < 64 || offtbl >= uint64(sz-32) {
		return 0, fmt.Errorf("%s: corrupt header", rd.fn)
	}

	return offtbl, nil
}
