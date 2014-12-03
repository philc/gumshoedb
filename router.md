# gumshoedb router

GumshoeDB Router is about the simplest way we can spread data across several nodes and collect it in when we
query.

## Sharding

We will shard data by interval. This is to maximize effectiveness of row compression and to lessen the amount
of work the router needs to do to merge results.

The router is provided with a list of shards (probably each is a different server, although maybe it would be
interesting to put many shards on one server to make it easier to move them around later). The mapping of
interval to shard is as follows

    shard index = (interval index) % (number of shards)

The 'interval index' is the number of the intervals that have occurred since the Unix epoch. For instance, if
the interval duration is 1 hour, then the interval index is for the interval starting at the beginning of Jan.
1, 2015 is 394464.

The sharding is *only* used by the router for inserting data. Queries go to all shards and are aggregated
properly no matter which shards return which data.

Initially a non-sharded DB will need to be sharded by hand. The changes from #8 help make that easier. It's
also valid to not shard the DB and introduce new, empty shards; the new data that's inserted will be spread
among all shards and eventually when the old data expires the shards will be balanced.

## Insertion

1. Unmarshal the JSON query.
2. Partition the RowMaps by shard, by truncating the row's timestamp and applying the modulus operation
   described above.
3. Marshal each set of RowMaps back into a new sub-insert-request. In parallel, send off to each shard. Report
   success when all of them are finished, or failure if any return a non-200.

## Queries

1. Parse the query (`gumshoe.ParseJSONQuery`).
2. Change the type of any `AggregateAvg` aggregates and replace to `AggregateSum`. (We need to compute
   averages at the end.)
3. Serialize the modified query and send to all shards in parallel.
4. When all results are received, unmarshal them.
5. Merge the results by summing the metrics and `rowCount`s. If there is grouping, the merge combines cells
   based on the value of the grouping column.
6. Create a new, synthesized result. Replace any previously added `AggregateSum` columns with the appropriate
   `AggregateAvg` (this is easy to compute now by dividing by the total `rowCount`).
7. In the result, set the `duration_ms` to the total elapsed time since the query was received.
8. Serialize the overall result and return to the client.

## Other considerations

The router will need to be provided with a copy of the DB config, or at least be initialized with the correct
interval duration to match the DB config.

Make sure the HTTP client is configured to maintain several open connections per shard.
