// dbwriter.go -- Constant DB built on top of the CHD MPH
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
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/dchest/siphash"
)

// Most data is serialized as big-endian integers. The exceptions are:
// Offset table:
//     This is mmap'd into the process and written as a little-endian uint64.
//     This is arguably an optimization -- most systems we work with are
//     little-endian. On big-endian systems, the DBReader code will convert
//     it on the fly to native-endian.

// DBWriter represents an abstraction to construct a read-only constant database.
// This database uses CHD as the underlying mechanism for constant time lookups
// of keys; keys and values are represented as arbitrary byte sequences ([]byte).
// The DB meta-data is protected by strong checksum (SHA512-256) and each stored value
// is protected by a distinct siphash-2-4.  Once all addition of key/val is complete,
// the DB is written to disk via the Freeze() function.
//
// We don't want to use SHA512-256 over the entire file - because it will mean reading
// a potentially large file in DBReader(). By using checksums separately per record, we
// increase the overhead a bit - but speeds up DBReader initialization for the common case;
// we will be verifying actual records opportunistically.
//
// The DB has the following general structure:
//   - 64 byte file header: big-endian encoding of all multibyte ints
//      * magic    [4]byte "CHDB"
//      * flags    uint32  for now, all zeros
//      * salt     [8]byte random salt for siphash record integrity
//      * nkeys    uint64  Number of keys in the DB
//      * offtbl   uint64  File offset of <offset, hash> table
//
//   - Contiguous series of records; each record is a key/value pair:
//      * cksum    uint64  Siphash checksum of value, offset (big endian)
//      * val      []byte  value bytes
//
//   - Possibly a gap until the next PageSize boundary (4096 bytes)
//   - Offset table: nkeys worth of offsets, hash pairs. Everything in this
//     table is little-endian encoded so we can mmap() it into memory.
//     Entry 'i' has two 64-bit words:
//      * offset in the file  where the corresponding value can be found
//      * hash key corresponding to the value
//   - Val_len table: nkeys worth of value lengths corresponding to each key.
//   - Marshaled Chd bytes (Chd:MarshalBinary())
//   - 32 bytes of strong checksum (SHA512_256); this checksum is done over
//     the file header, offset-table and marshaled chd.
type DBWriter struct {
	fd *os.File
	bb *ChdBuilder

	// to detect duplicates
	keymap map[uint64]*value

	// siphash key: just binary encoded salt
	salt []byte

	// running count of current offset within fd where we are writing
	// records
	off uint64

	fntmp  string // tmp file name
	fn     string // final file holding the PHF
	frozen bool
}

// things associated with each key/value pair
type value struct {
	off  uint64
	vlen uint32
}

// NewDBWriter prepares file 'fn' to hold a constant DB built using
// CHD minimal perfect hash function. Once written, the DB is "frozen"
// and readers will open it using NewDBReader() to do constant time lookups
// of key to value.
func NewDBWriter(fn string) (*DBWriter, error) {
	bb, err := New()
	if err != nil {
		return nil, err
	}

	tmp := fmt.Sprintf("%s.tmp.%d", fn, rand32())
	fd, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}

	w := &DBWriter{
		fd:     fd,
		bb:     bb,
		keymap: make(map[uint64]*value),
		salt:   randbytes(16),
		off:    64, // starting offset past the header
		fn:     fn,
		fntmp:  tmp,
	}

	// Leave some space for a header; we will fill this in when we
	// are done Freezing.
	var z [64]byte
	if _, err := writeAll(fd, z[:]); err != nil {
		return nil, err
	}

	return w, nil
}

// Len returns the total number of distinct keys in the DB
func (w *DBWriter) Len() int {
	return len(w.keymap)
}

// AddKeyVals adds a series of key-value matched pairs to the db. If they are of
// unequal length, only the smaller of the lengths are used. Records with duplicate
// keys are discarded.
// Returns number of records added.
func (w *DBWriter) AddKeyVals(keys []uint64, vals [][]byte) (int, error) {
	if w.frozen {
		return 0, ErrFrozen
	}

	n := len(keys)
	if len(vals) < n {
		n = len(vals)
	}

	var z int
	for i := 0; i < n; i++ {
		if ok, err := w.addRecord(keys[i], vals[i]); err != nil {
			return z, err
		} else if ok {
			z++
		}
	}

	return z, nil
}

// Adds adds a single key,value pair.
func (w *DBWriter) Add(key uint64, val []byte) error {
	if w.frozen {
		return ErrFrozen
	}

	return w.addRecord(key, val)
}

// Freeze builds the minimal perfect hash, writes the DB and closes it. The parameter
// 'load' controls the MPHF table size (load): 0 < load < 1.
// If space is not an issue, use a lower value of load. Typical values are between
// 0.75 and 0.9.
func (w *DBWriter) Freeze(load float64) (err error) {
	defer func() {
		// undo the tmpfile
		if err != nil {
			w.fd.Close()
			os.Remove(w.fntmp)
		}
	}()

	if w.frozen {
		return ErrFrozen
	}

	chd, err := w.bb.Freeze(load)
	if err != nil {
		return ErrMPHFail
	}

	// calculate strong checksum for all data from this point on.
	h := sha512.New512_256()

	tee := io.MultiWriter(w.fd, h)

	// We align the offset table to pagesize - so we can mmap it when we read it back.
	pgsz := uint64(os.Getpagesize())
	pgsz_m1 := pgsz - 1
	offtbl := w.off + pgsz_m1
	offtbl &= ^pgsz_m1

	if offtbl > w.off {
		zeroes := make([]byte, offtbl-w.off)
		if _, err = writeAll(w.fd, zeroes); err != nil {
			return err
		}
		w.off = offtbl
	}

	// Now offset is at a page boundary.

	var ehdr [64]byte

	// header is encoded in big-endian format
	be := binary.BigEndian
	copy(ehdr[:4], []byte{'C', 'H', 'D', 'B'})

	// 8 = 4 bytes magic + skip 4 bytes of flags (zero for now)
	i := 8

	i += copy(ehdr[i:], w.salt)
	be.PutUint64(ehdr[i:i+8], uint64(chd.Len()))
	i += 8
	be.PutUint64(ehdr[i:i+8], offtbl)

	// add header to checksum
	h.Write(ehdr[:])

	// write to file and checksum together
	if err := w.marshalOffsets(tee, chd); err != nil {
		return err
	}

	// align the offset to next 64 bit boundary
	offtbl = w.off + 7
	offtbl &= ^uint64(7)
	if offtbl > w.off {
		zeroes := make([]byte, offtbl-w.off)
		if _, err = writeAll(tee, zeroes); err != nil {
			return err
		}
		w.off = offtbl
	}

	// Next, we now encode the chd and write to disk.
	nw, err := chd.MarshalBinary(tee)
	if err != nil {
		return err
	}
	w.off += uint64(nw)

	// Trailer is the checksum of everything
	cksum := h.Sum(nil)
	if _, err := writeAll(w.fd, cksum[:]); err != nil {
		return err
	}

	// Finally, write the header at start of file
	w.fd.Seek(0, 0)
	if _, err := writeAll(w.fd, ehdr[:]); err != nil {
		return err
	}

	w.frozen = true
	w.fd.Sync()
	w.fd.Close()

	return os.Rename(w.fntmp, w.fn)
}

// Abort stops the construction of the perfect hash db
func (w *DBWriter) Abort() {
	w.fd.Close()
	os.Remove(w.fntmp)
}

// write the offset mapping table and value-len table
func (w *DBWriter) marshalOffsets(tee io.Writer, c *Chd) error {
	n := uint64(c.Len())
	offset := make([]uint64, 2*n)
	vlen := make([]uint32, n)

	for k, r := range w.keymap {
		i := c.Find(k)

		vlen[i] = r.vlen

		// each entry is 2 64-bit words
		j := i * 2
		offset[j] = r.off
		offset[j+1] = k
	}

	bs := u64sToByteSlice(offset)
	if _, err := writeAll(tee, bs); err != nil {
		return err
	}

	// Now write the value-length table
	bs = u32sToByteSlice(vlen)
	if _, err := writeAll(tee, bs); err != nil {
		return err
	}

	w.off += uint64(n * (8 + 8 + 4))
	return nil
}

// compute checksums and add a record to the file at the current offset.
func (w *DBWriter) addRecord(key uint64, val []byte) (bool, error) {
	if uint64(len(val)) > uint64(1<<32)-1 {
		return false, ErrValueTooLarge
	}

	_, ok := w.keymap[key]
	if ok {
		return false, ErrExists
	}

	// first add to the underlying PHF constructor
	if err := w.bb.Add(key); err != nil {
		return false, err
	}

	v := &value{
		off:  w.off,
		vlen: uint32(len(val)),
	}
	w.keymap[key] = v

	// Don't write values if we don't need to
	if len(val) > 0 {
		if err := w.writeRecord(val, v.off); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (w *DBWriter) writeRecord(val []byte, off uint64) error {
	var o [8]byte
	var c [8]byte

	be := binary.BigEndian
	be.PutUint64(o[:], off)

	h := siphash.New(w.salt)
	h.Write(o[:])
	h.Write(val)
	be.PutUint64(c[:], h.Sum64())

	// Checksum at the start of record
	if _, err := writeAll(w.fd, c[:]); err != nil {
		return err
	}

	if _, err := writeAll(w.fd, val); err != nil {
		return err
	}

	w.off += uint64(len(val)) + 8
	return nil
}

// cleanup intermediate work and return an error instance
func (w *DBWriter) error(f string, v ...interface{}) error {
	w.fd.Close()
	os.Remove(w.fntmp)

	return fmt.Errorf(f, v...)
}

func writeAll(w io.Writer, buf []byte) (int, error) {
	n, err := w.Write(buf)
	if err != nil {
		return 0, err
	}
	if n != len(buf) {
		return n, errShortWrite(n)
	}
	return n, nil
}
