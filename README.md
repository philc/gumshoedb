
Implementation plan

* How can I cast a byte buffer into a struct?
* If I have a pointer to a value and know its type, how can I pick the right operator to use with it?

Performance ideas
* Use pointer arithmetic to iterate through arrays of dynamic size (e.g. column filter count)
* Use vectors instead of hashmaps for low-cardinality group-bys

Performance results
* Assigning to hashes (for grouping) is much slower than grouping by array.
* Iterating over two-dimensional slices is twice as slow as simple arrays.
