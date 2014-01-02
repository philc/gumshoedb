
Implementation plan

* How can I cast a byte buffer into a struct?
* If I have a pointer to a value and know its type, how can I pick the right operator to use with it?

Performance ideas
-----------------
* Use pointer arithmetic to iterate through rows of variable width
* Use vectors instead of hashmaps for low-cardinality group-bys

High-level performance observations
-----------------------------------
* Assigning to hashes (for grouping) is much slower than grouping by array.
* Iterating over two-dimensional slices is twice as slow as simple arrays.


Query API design
================
* Table
* List of filters
* List of GroupBy columns
* Selected aggregates, and their names

Inspiration:
https://github.com/metamx/druid/wiki/Querying
https://github.com/metamx/druid/wiki/GroupByQuery

Representative query:

{
  table: "events",
  "aggregates":[
      {"type": "sum", "name": "countrySum", "column": "country"},
      {"type": "sum", "name": "atSum", "column": "at"}],
  "filters": [{"type": "greaterThan", "column": "at", "value": 2}],
  "groupings": [{"column": "country", "name":"country"}]
}


Result:

{
  results:
    [{date: "2013-12-01", country: "Japan", clicks: 123, rowCount: 145},
     {date: "2013-12-01", country: "USA", clicks: 123, rowCount: 145},
     {date: "2013-12-02", country: "Japan", clicks: 123, rowCount: 145},
     {date: "2013-12-02", country: "USA", clicks: 123, rowCount: 145}]
}

Resources

[Memory mapped files API]([http://www.gnu.org/software/libc/manual/html_node/Memory_002dmapped-I_002fO.html)
