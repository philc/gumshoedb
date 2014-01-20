Getting started
---------------

Run the tests:

    make run-tests

Run the benchmarks:

    make run-benchmarks

Major todos
-----------
* All columns are currently assumed to be float32. Use byte arrays rather than typed arrays, so we mix types
  in the row and use the smallest possible type needed for a given column. The query time scales linearly with
  the bit-width of the row.
* Make the schema configurable via a config file. Right now it's hard coded into the code (which is necessary
  since we're using native arrays, not slices, for performance reasons). A schema which is defined at runtime
  will become easier if we support byte arrays for rows.
* Support nullable columns, e.g. differentiate between "null" and zero column values. We could use a magic
  number to represent null.
* Use arrays for storing results for low-cardinality group-bys, and hashmaps for high-cardinality group-bys
  (assuming there's a large performance difference between arrays and hashmaps in the low-cardinality case,
  which is the common case).
* Expose metrics via HTTP routes so that summary metrics of GumshoeDB's data set are easy to inspect.

Using benchmarks
----------------
The benchmark suite is a critical tool for evaluating different implementation strategies.

To run:

    make run-benchmarks

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
