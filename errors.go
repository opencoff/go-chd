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
	"errors"
	"fmt"
)

func errShortWrite(n int) error {
	return fmt.Errorf("chd: incomplete write; exp 8, saw %d", n)
}

var (
	// ErrMPHFail is returned when the gamma value provided to Freeze() is too small to
	// build a minimal perfect hash table.
	ErrMPHFail = errors.New("failed to build MPH")

	// ErrFrozen is returned when attempting to add new records to an already frozen DB
	// It is also returned when trying to freeze a DB that's already frozen.
	ErrFrozen = errors.New("DB already frozen")

	// ErrValueTooLarge is returned if the value-length is larger than 2^32-1 bytes
	ErrValueTooLarge = errors.New("value is larger than 2^32-1 bytes")

	// ErrExists is returned if a duplicate key is added to the DB
	ErrExists = errors.New("key exists in DB")

	// ErrNoKey is returned when a key cannot be found in the DB
	ErrNoKey = errors.New("No such key")
)
