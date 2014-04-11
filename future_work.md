## Ideas for future development

* Like levelDB, we can write an insertion log to disk and then delete it when we flush. This allows for crash
  recovery of the stuff in the memtable.
* For comparing/sorting b-tree keys (which is the dimension portion of a row), we're just using simple
  byte-wise comparison for now. We could do real semantic comparison later if we had any need to do range
  requests.
* Switching to a column-oriented design will almost certainly provide a huge speedup for our common-case
  queries. In particular this allows for much better compression and skipping much more data while scanning.
