## New insertion strategy

* Change FactTable -> DB
* DB is composed of :
  - Schema (unchanging)
  - State (immutable; ro mmap; swapped out atomically)
  - MemTable (b-tree backed sorted input data that's periodically flushed -- only touched by the inserter
    goroutine)
  - Request chan
  - Flush chan
  - Shutdown chan
* State contains the intervals/segments, dimension tables, row count, request chan
* States and MemTables both have a reference to the schema, for convenience.
* There is a single request goroutine which has a reference to the active state. All requests must be funneled
  through a db-global request chan which is emptied by the request goroutine.
* MemTable contains one b-tree per interval with active data, a dimension table for all new dimension values
* There is a buffered chan for the insertion queue. This being full provides backpressure for insertion.
* The inserter goroutine pulls from the insert chan and pushes into the memtable
* Every N minutes, we flush (do this from the inserter goroutine):
  - Only need to update intervals that have a non-empty b-tree in this memtable
  - Process the b-tree and interval in lock-step, writing out to a new set of segment files (append timestamp)
  - Create a new master dimension table by appending the memtable's dimension table to the old one
  - Make a new State that references the new set of intervals/filenames and new dimension table
  - Make a list of intervals from the old state that aren't needed any more. Send the new state and the list
    of to-be-garbage-collected intervals over to the request routine on the flush chan.
* The request goroutines pushes onto the current State's request chan
  - A fixed number of goroutines pull requests off the request chan and answer them.
  - When it's time to flush, a new set of request goroutines is spun up for the new state. Then the old
    state's request chan is closed.
  - Then a garbage collection goroutine is spun up for the old State with the list of to-be-collected
    intervals. It waits for all the old request goroutines to exit and then collects.
* State garbage collection
  - Close all segment file handles
  - Delete segment files in Intervals that are not referenced in the subsequent state.
* Clean shutdown: listen for signals and flush before shutdown
  - On shutdown, close shutdown chan
  - flush, then exit
* Regular "flushing" which was previously necessary is no longer required because the State files are
  immutable.

## Goroutine lifecycles

* Single inserter goroutine
  - Created by Open() when a DB is created
  - When the shutdown chan is closed, the inserter flushes, then exits
* Single request goroutine
  - Created by Open() when a DB is created
  - When the shutdown chan is closed, the request goroutine exits -- in this case, it should wait for the
    cleanup goroutine to finisih (although it normally shouldn't)
* N-per-State request goroutines
  - Receive off the per-State request chan; exit after closed
* Cleanup goroutine
  - Spawned by the request goroutine after all requests have been processed for the old state.

## Changes to persistence

* States are static on disk. They are just the immutable mmaped files and a big list for the dimension tables.
* Dimension tables are reconstructed on boot from a list (no need to persist the map)
* For the schema, store: timestamp column, dimension columns, metric columns
* The saved DB includes the saved schema and the saved state.
* On restore from disk, check the given schema vs. the stored one

## Notes

* Two flushes are prevented from happening concurrently because they are processed by pulling from the
  signalFlushes chan in serial by the inserter.

## To Do in the future

* Like levelDB, we can write an insertion log to disk and then delete it when we flush. This allows for crash
  recovery of the stuff in the memtable.
* For comparing/sorting b-tree keys (which is the dimension portion of a row), we're just using simple
  byte-wise comparison for now. We could do real semantic comparison later if we had any need to do range
  requests.







I have essentially a database with a single writer goroutine and many reader goroutines. The database state is
encapsulated in a State struct (better names very much welcome). A State is readonly and is swapped out
periodically (think every few minutes) with a new State. So far, pretty simple -- I could just use an RWMutex
to protect the state.

The twist is that the old State requires some garbage collection after it is done being used. It's okay if
queries are still running on the old State after it has been swapped out, but after they are all done I need
to do some cleanup on that State (basically close some file handles and delete some files). I have a per-State
dispatcher goroutine that is aware of 


Other possibly-relevant info: the database is very low qps, but each query is relatively expensive (so
introducing O(queries) operations is no big deal).
