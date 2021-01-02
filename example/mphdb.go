// mphdb.go -- Build a Constant DB based on BBHash MPH
//
// Author: Sudhi Herle <sudhi@herle.net>
//
// This software does not come with any express or implied
// warranty; it is provided "as is". No claim  is made to its
// suitability for any purpose.

// mphdb.go is an example of using bbhash:DBWriter() and DBReader.
// One can construct the on-disk MPH DB using a variety of input:
//   - white space delimited text file: first field is key, second field is value
//   - Comma Separated text file (CSV): first field is key, second field is value
//
// Sometimes, bbhash gets into a pathological state while constructing MPH out of very
// large data sets. This can be alleviated by using a larger "gamma". mphdb tries to
// bump the gamma to "4.0" whenever we have more than 1M keys.

package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/opencoff/go-chd"

	flag "github.com/opencoff/pflag"
)

type value struct {
	hash uint64
	key  string
	val  string
}

func main() {
	var load float64
	var verify bool

	usage := fmt.Sprintf("%s [options] OUTPUT [INPUT ...]", os.Args[0])

	flag.Float64VarP(&load, "load", "l", 0.85, "Use `L` as the hash table load factor")
	flag.BoolVarP(&verify, "verify", "V", false, "Verify a constant DB")
	flag.Usage = func() {
		fmt.Printf("mphdb - create MPH DB from txt or CSV files using CHD\nUsage: %s\n", usage)
		flag.PrintDefaults()
	}

	flag.Parse()
	args := flag.Args()

	if len(args) < 1 {
		die("No output file name!\nUsage: %s\n", usage)
	}

	fn := args[0]
	args = args[1:]

	if verify {
		db, err := chd.NewDBReader(fn, 1000)
		if err != nil {
			die("Can't read %s: %s", fn, err)
		}

		fmt.Printf("%s: %d records\n", fn, db.Len())
		db.Close()
		return
	}

	db, err := chd.NewDBWriter(fn)
	if err != nil {
		die("can't create MPH DB: %s", err)
	}

	var n uint64
	if len(args) > 0 {
		for _, f := range args {
			switch {
			case strings.HasSuffix(f, ".txt"):
				n, err = AddTextFile(db, f, " \t")

			case strings.HasSuffix(f, ".csv"):
				n, err = AddCSVFile(db, f, ',', '#', 0, 1)

			default:
				warn("Don't know how to add %s", f)
				continue
			}

			if err != nil {
				warn("can't add %s: %s", f, err)
				continue
			}

			fmt.Printf("+ %s: %d records\n", f, n)
		}
	} else {
		n, err = AddTextStream(db, os.Stdin, " \t")
		if err != nil {
			db.Abort()
			die("can't add STDIN: %s", err)
		}

		fmt.Printf("+ <STDIN>: %d records\n", n)
	}

	err = db.Freeze(load)
	if err != nil {
		db.Abort()
		die("can't write db %s: %s", fn, err)
	}
}

// die with error
func die(f string, v ...interface{}) {
	warn(f, v...)
	os.Exit(1)
}

func warn(f string, v ...interface{}) {
	z := fmt.Sprintf("%s: %s", os.Args[0], f)
	s := fmt.Sprintf(z, v...)
	if n := len(s); s[n-1] != '\n' {
		s += "\n"
	}

	os.Stderr.WriteString(s)
	os.Stderr.Sync()
}

// vim: ft=go:sw=4:ts=4:noexpandtab:tw=78:
