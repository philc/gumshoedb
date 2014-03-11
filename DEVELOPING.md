Getting started
---------------
First, install [glp](https://github.com/cespare/glp). We use this to manage dependency pinning for this
project. After you get the dependencies using `glp sync`, usage is similar to the Go tool itself.

    # Build the server
    glp build -o bin/server server
    # Run all the tests:
    glp test ./...

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

    glp test -run=NONE -bench=. gumshoe synthetic

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
