// db_test.go -- test suite for dbreader/dbwriter
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
	"flag"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/opencoff/go-fasthash"
)

var keep bool

func init() {
	flag.BoolVar(&keep, "keep", false, "Keep test DB")
}

func TestDB(t *testing.T) {
	assert := newAsserter(t)

	fn := fmt.Sprintf("%s/mph%d.db", os.TempDir(), rand.Int())

	wr, err := NewDBWriter(fn)
	assert(err == nil, "can't create db: %s", err)

	defer func() {
		if keep {
			t.Logf("DB in %s retained after test\n", fn)
		} else {
			os.Remove(fn)
		}
	}()

	hseed := rand64()
	kvmap := make(map[uint64]string)
	for _, s := range keyw {
		h := fasthash.Hash64(hseed, []byte(s))
		err = wr.Add(h, []byte(s))
		assert(err == nil, "can't add key %x: %s", h, err)
		kvmap[h] = s
	}

	err = wr.Freeze(0.9)
	assert(err == nil, "freeze failed: %s", err)

	rd, err := NewDBReader(fn, 10)
	assert(err == nil, "read failed: %s", err)

	for h, v := range kvmap {
		s, err := rd.Find(h)
		assert(err == nil, "can't find key %#x: %s", h, err)

		assert(string(s) == v, "key %s: value mismatch; exp %s, saw %s", h, v, string(s))
	}

	// now look for keys not in the DB
	for i := 0; i < 10; i++ {
		v, err := rd.Find(uint64(i))
		assert(err != nil, "whoa: found key %d => %s", i, string(v))
	}
}
