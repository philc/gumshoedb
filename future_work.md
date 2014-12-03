## Ideas for future development

* Like levelDB, we can write an insertion log to disk and then delete it when we flush. This allows for crash
  recovery of the stuff in the memtable.
* For comparing/sorting b-tree keys (which is the dimension portion of a row), we're just using simple
  byte-wise comparison for now. We could do real semantic comparison later if we had any need to do range
  requests.
* Switching to a column-oriented design will almost certainly provide a huge speedup for our common-case
  queries. In particular this allows for much better compression and skipping much more data while scanning.
* We can switch to using varints rather than our cornucopia of integer types. This would make our rows
  non-fixed-size, though. (This makes more sense if we're column-oriented.)

## Optimizations

* See if removing all the `:=` (predeclaring everything) in the inner loops helps
* Implement slice grouping
  - Use slice grouping for dimension columsn with fewer than N values (say N = 1M)
  - Preallocate all the slots
  - See if axing the (Row,Metric)Bytes conversions helps (probably not)
* Presize the grouping maps
* Research how GROUP BY queries are implemented in other DBs
* Skip data in the `falseFilterFunc` case
