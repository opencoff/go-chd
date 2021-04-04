[![GoDoc](https://godoc.org/github.com/opencoff/go-chd?status.svg)](https://godoc.org/github.com/opencoff/go-chd)
[![Go Report Card](https://goreportcard.com/badge/github.com/opencoff/go-chd)](https://goreportcard.com/report/github.com/opencoff/go-chd)

# go-chd - Minimal Perfect Hash Function using Compress Hash Displace

## What is it?
A library to create, query and serialize/de-serialize minimal perfect hash function ("MPHF").

This is an implementation of [CHD](http://cmph.sourceforge.net/papers/esa09.pdf) -
inspired by this [gist](https://gist.github.com/pervognsen/b21f6dd13f4bcb4ff2123f0d78fcfd17).

The library exposes the following types:

- `ChdBuilder`: Represents the construction phase of the MPHF.
  function as described in the paper above.
- `Chd`: Represents a frozen MPHF over a given set of keys. You can only
  do lookups on this type.
- `DBWriter`: Used to construct a constant database of key-value
  pairs - where the lookup of a given key is done in constant time
  using `ChdBuilder`. Essentially, this type serializes a collection
  of key-value pairs using `ChdBuilder` as the underlying index.
- `DBReader`: Used for looking up key-values from a previously
  constructed (serialized) database.

*NOTE* Minimal Perfect Hash functions take a fixed input and
generate a mapping to lookup the items in constant time. In
particular, they are NOT a replacement for a traditional hash-table;
i.e., it may yield false-positives when queried using keys not
present during construction. In concrete terms:

   Let S = {k0, k1, ... kn}  be your input key set.

   If H: S -> {0, .. n} is a minimal perfect hash function, then
   H(kx) for kx NOT in S may yield an integer result (indicating
   that kx was successfully "looked up").

Thus, if users of `Chd` are unsure of the input being passed to such a
`Lookup()` function, they should add an additional comparison against
the actual key to verify. Look at `dbreader.go:Find()` for an
example.

`DBWriter` optimizes the database if there are no values present -
i.e., keys-only. This optimization significantly reduces the
file-size.


## How do I use it?
Like any other golang library: `go get github.com/opencoff/go-chd`.

## Example Program
There is a working example of the `DBWriter` and `DBReader` interfaces in the
file *example/mphdb.go*. This example demonstrates the following functionality:

- add one or more space delimited key/value files (first field is key, second
  field is value)
- add one or more CSV files (first field is key, second field is value)
- Write the resulting MPH DB to disk
- Read the DB and verify its integrity

First, lets run some tests and make sure chd is working fine:

```sh

  $ git clone https://github.com/opencoff/go-chd
  $ cd go-chd
  $ make test

```

Now, lets build and run the example program:
```sh

  $ make
  $ ./mphdb -h
```

There is a helper python script to generate a very large text file of
hostnames and IP addresses: `genhosts.py`. You can run it like so:

```sh

  $ python ./example/genhosts.py 192.168.0.0/16 > a.txt
```

The above example generates 65535 hostnames and corresponding IP addresses; each of the
IP addresses is sequentially drawn from the given subnet.

**NOTE** If you use a "/8" subnet mask you will generate a _lot_ of data (~430MB in size).

Once you have the input generated, you can feed it to the `example` program above to generate
a MPH DB:
```sh

  $ ./mphdb foo.db a.txt
  $ ./mphdb -V foo.db
```

It is possible that "mphdb" fails to construct a DB after trying 1,000,000 times. In that case,
try lowering the "load" factor (default is 0.85).

```sh
  $ ./mphdb -l 0.75 foo.db a.txt
```

## Basic Usage of ChdBuilder
Assuming you have read your keys, hashed them into `uint64`, this is how you can use the library:

```go

        builder, err := chd.New(0.9)
        if err != nil { panic(err) }

        for i := range keys {
            builder.Add(keys[i])
        }

        lookup, err := builder.Freeze()

        // Now, call Find() with each key to gets its unique mapping.
        // Note: Find() returns values in the range closed-interval [1, len(keys)]
        for i, k := range keys {
                j := lookup.Find(k)
                fmt.Printf("%d: %#x maps to %d\n", i, k, j)
        }

```

## Writing a DB Once, but lookup many times
One can construct an on-disk constant-time lookup using `ChdBuilder` as
the underlying indexing mechanism. Such a DB is useful in situations
where the key/value pairs are NOT changed frequently; i.e.,
read-dominant workloads. The typical pattern in such situations is
to build the constant-DB _once_ for efficient retrieval and do
lookups multiple times.

The example program in `example/` has helper routines to add from a
text or CSV delimited file: see `example/text.go`.

## Implementation Notes

* `chd.go`: The main implementation of the CHD algorithm. It has two
  types: one to construct and freeze a MPHF (`ChdBuilder`) and
  another to do constant time lookups from a frozen CHD MPHF
  (`Chd`).

* `dbwriter.go`: Create a read-only, constant-time MPH lookup DB. It 
  can store arbitrary byte stream "values" - each of which is
  identified by a unique `uint64` key. The DB structure is optimized
  for reading on the most common architectures - little-endian:
  amd64, arm64 etc.

* `dbreader.go`: Provides a constant-time lookup of a previously
  constructed CHD MPH DB. DB reads use `mmap(2)` to reduce I/O
  bottlenecks. For little-endian architectures, there is no data
  "parsing" of the lookup tables, offset tables etc. They are 
  interpreted in-situ from the mmap'd data. To keep the code
  generic, every multi-byte int is converted to little-endian order
  before use. These conversion routines are in `endian_XX.go`.

* `mmap.go`: Utility functions to map byte-slices to uintXX slices
  and vice versa.

* `marshal.go`: Marshal/Unmarshal CHD MPH

## License
GPL v2.0
