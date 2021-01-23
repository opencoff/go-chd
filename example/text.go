// text.go -- read from variety of text files and populate a CHD DBWriter

package main

import (
	"bufio"
	"encoding/csv"
	"io"
	"os"
	"strings"

	"github.com/opencoff/go-chd"
	"github.com/opencoff/go-fasthash"
)

type record struct {
	key uint64
	val []byte
}

// AddTextFile adds contents from text file 'fn' where key and value are separated
// by one of the characters in 'delim'. Duplicates, Empty lines or lines with no value
// are skipped. This function just opens the file and calls AddTextStream()
// Returns number of records added.
func AddTextFile(w *chd.DBWriter, fn string, delim string) (uint64, error) {
	fd, err := os.Open(fn)
	if err != nil {
		return 0, err
	}

	if len(delim) == 0 {
		delim = " \t"
	}

	defer fd.Close()

	return AddTextStream(w, fd, delim)
}

// AddTextStream adds contents from text stream 'fd' where key and value are separated
// by one of the characters in 'delim'. Duplicates, Empty lines or lines with no value
// are skipped.
// Returns number of records added.
func AddTextStream(w *chd.DBWriter, fd io.Reader, delim string) (uint64, error) {
	rd := bufio.NewReader(fd)
	sc := bufio.NewScanner(rd)
	ch := make(chan *record, 10)

	// do I/O asynchronously
	go func(sc *bufio.Scanner, ch chan *record) {
		var empty string

		for sc.Scan() {
			s := strings.TrimSpace(sc.Text())
			if len(s) == 0 || s[0] == '#' {
				continue
			}

			var k, v string

			// if we have no delimiters - we treat the value as "boolean"
			i := strings.IndexAny(s, delim)
			if i > 0 {
				k = s[:i]
				v = s[i:]
			} else {
				k = s
				v = empty
			}

			// ignore items that are too large
			if len(v) >= 4294967295 {
				continue
			}

			ch <- makeRecord(k, v)
		}

		close(ch)
	}(sc, ch)

	return addFromChan(w, ch)
}

// AddCSVFile adds contents from CSV file 'fn'. If 'kwfield' and 'valfield' are
// non-negative, they indicate the field# of the key and value respectively; the
// default value for 'kwfield' & 'valfield' is 0 and 1 respectively.
// If 'comma' is not 0, the default CSV delimiter is ','.
// If 'comment' is not 0, then lines beginning with that rune are discarded.
// Records where the 'kwfield' and 'valfield' can't be evaluated are discarded.
// Returns number of records added.
func AddCSVFile(w *chd.DBWriter, fn string, comma, comment rune, kwfield, valfield int) (uint64, error) {
	fd, err := os.Open(fn)
	if err != nil {
		return 0, err
	}

	defer fd.Close()

	return AddCSVStream(w, fd, comma, comment, kwfield, valfield)
}

// AddCSVStream adds contents from CSV file 'fn'. If 'kwfield' and 'valfield' are
// non-negative, they indicate the field# of the key and value respectively; the
// default value for 'kwfield' & 'valfield' is 0 and 1 respectively.
// If 'comma' is not 0, the default CSV delimiter is ','.
// If 'comment' is not 0, then lines beginning with that rune are discarded.
// Records where the 'kwfield' and 'valfield' can't be evaluated are discarded.
// Returns number of records added.
func AddCSVStream(w *chd.DBWriter, fd io.Reader, comma, comment rune, kwfield, valfield int) (uint64, error) {
	if kwfield < 0 {
		kwfield = 0
	}

	if valfield < 0 {
		valfield = 1
	}

	var max int = valfield
	if kwfield > valfield {
		max = kwfield
	}

	max += 1

	ch := make(chan *record, 10)
	cr := csv.NewReader(fd)
	cr.Comma = comma
	cr.Comment = comment
	cr.FieldsPerRecord = -1
	cr.TrimLeadingSpace = true
	cr.ReuseRecord = true

	go func(cr *csv.Reader, ch chan *record) {
		for {
			v, err := cr.Read()
			if err != nil {
				break
			}

			if len(v) < max {
				continue
			}

			ch <- makeRecord(v[kwfield], v[valfield])
		}
		close(ch)
	}(cr, ch)

	return addFromChan(w, ch)
}

// read partial records from the chan, complete them and write them to disk.
// Build up the internal tables as we go
func addFromChan(w *chd.DBWriter, ch chan *record) (uint64, error) {
	var n uint64
	for r := range ch {
		if err := w.Add(r.key, r.val); err != nil {
			return n, err
		}
		n++
	}

	return n, nil
}

// XXX We really ought to use a proper salt for this keyed-hash function.
// But then where we would store the salt!
func makeRecord(key, val string) *record {
	h := fasthash.Hash64(0, []byte(key))
	return &record{h, []byte(val)}
}
