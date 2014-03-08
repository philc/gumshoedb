Getting started
---------------

Run the tests:

    make test

Run the benchmarks:

    make benchmark

Major todos
-----------
* Use arrays for storing results for low-cardinality group-bys, and hashmaps for high-cardinality group-bys
  (assuming there's a large performance difference between arrays and hashmaps in the low-cardinality case,
  which is the common case).
* Expose metrics via HTTP routes so that summary metrics of GumshoeDB's data set are easy to inspect.

Using benchmarks
----------------
The benchmark suite is a critical tool for evaluating different implementation strategies.

To run:

    make benchmark
    make synthetic-benchmark

The synthetic suite benchmarks small, narrow techniques and represents the upper-bound of performance. It
provides a clean, isolated view on how fast a technique is.

The core benchmarks test the core GumshoeDB code paths. The core code paths should be comparable in speed to
the ideal benchmarks -- ideally within 20%.

High level performance observations
-----------------------------------
* Iterating over two-dimensional slices is twice as slow as contiguous slices/arrays, because of pointer
  indirection.
* Scan speed scales linearly with the bit-width of the row

REST API
--------
The query API JSON format is inspired by [Druid's](https://github.com/metamx/druid/wiki/Querying).

Go-localpath
------------
Install and use [go-localpath](https://github.com/cespare/go-localpath) for greater ease of development. This
is a small tool that lets you call the `go` tool with a modified `$GOPATH` (the way the Makefile does).
go-localpath uses the `.glp` file to know what to do.

If you're using go-localpath (and you've set it up to replace the `go` command -- see directions on the
go-localpath readme), then you can use the go tool directly:

    $ go build -o build/gumshoe_server server
    $ go test -run=Persistence gumshoe
    $ go test -run=NONE -bench=Parallel synthetic
