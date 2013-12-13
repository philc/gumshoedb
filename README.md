
Implementation plan

* How can I cast a byte buffer into a struct?
* If I have a pointer to a value and know its type, how can I pick the right operator to use with it?

Performance ideas
* Use pointer arithmetic to iterate through arrays of dynamic size (e.g. column filter count)
* Use vectors instead of hashmaps for low-cardinality group-bys

Performance results
* Assigning to hashes (for grouping) is much slower than grouping by array.
* Iterating over two-dimensional slices is twice as slow as simple arrays.


Query API design

* Table
* List of filters
* List of GroupBy columns
* Selected aggregates, and their names

What should the result set look like?

https://github.com/metamx/druid/wiki/Querying

https://github.com/metamx/druid/wiki/GroupByQuery

Representative query:

{
  table: "events",
  filters: [{type:"greaterThan", column:"time_bucket", value: 22}],
  groupBy: [{timeFunction:"day", column:"time_bucket", name: "date"}, {column:"country"}],
  aggregates: [{type: "sum", name: "clicks", column: "click"}]
}

Result:

{
  results:
    [{date: "2013-12-01", country: "Japan", clicks: 123, count: 145},
     {date: "2013-12-01", country: "USA", clicks: 123, count: 145},
     {date: "2013-12-02", country: "Japan", clicks: 123, count: 145},
     {date: "2013-12-02", country: "USA", clicks: 123, count: 145}]
}

Notes:
* Averages can be done by the caller (dividing one column with the count).
