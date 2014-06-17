GumshoeDB
=========

GumshoeDB is a database for quickly scouring hundreds of millions of analytics events and finding
answers. It is has a similar design to the Dremel and Druid data stores.

It's a work-in-progress and not yet documented for public consumption. Details about typical use cases and key
design choices are coming soon.

To run:

* Install [glp](https://github.com/cespare/glp)
* `glp sync`
* `glp build -o bin/server server`
* `./server`

This starts a GumshoeDB daemon at [localhost:9000](http://localhost:9000).

GumshoeDB can be interacted with over HTTP. Test data can be imported into the database with a PUT request:

    curl -iX PUT 'localhost:9000/insert' -d '
    [{"clicks": 1, "age": 21, "name": "Starbuck", "country": "USA"},
     {"clicks": 2, "age": 22, "name": "Boomer", "country": "USA"},
     {"clicks": 3, "age": 23, "name": "Helo", "country": "CAN"},
     {"clicks": 4, "age": 24, "name": "Apollo", "country": "DEU"}
     ]'

Here's a representative query, assuming the columns "country", "age", and "clicks".

    curl -iX POST localhost:9000/tables/facts/query -d '
    {
      "aggregates":[
          {"type": "sum", "name": "clicks", "column": "clicks"},
          {"type": "average", "name": "avgAge", "column": "age"}],
      "filters": [{"type": ">", "column": "age", "value": 20},
                  {"type": "in", "column": "country", "value": ["USA", "CAN"]}],
      "groupings": [{"column": "country", "name":"country"}]
    }
    '

    Results:
    {
      "duration": 0,
      "results":
        [{"avgAge": 21.5, "clicks": 3, "country": "USA", "rowCount": 2},
         {"avgAge": 23, "clicks": 3, "country": "CAN", "rowCount": 1}]
    }

See [DEVELOPING.md](https://github.com/philc/gumshoedb/blob/master/DEVELOPING.md) for how to navigate the code
and make changes.

Gumshoedb is licensed under [the MIT license](http://www.opensource.org/licenses/mit-license.php).

Data Model
==========

A GumshoeDB database is logically similar a single table in a relational database: there is a schema, which
specifies fixed columns, and there are many rows which follow that schema. There are two kinds of columns:
*dimensions* and *metrics*. Dimensions are attributes of the data, and the values may be strings or numeric
types. Metrics are numeric counts (floating-point or integer types).

When new data is inserted into GumshoeDB, each row must be associated with a timestamp. The data in a
GumshoeDB database is grouped into sequential, non-overlapping time intervals (right now, one hour -- this
will be configurable in the future). Queries will return data at this granularity.

Implementation
==============

Internally, each interval in a GumshoeDB database is divided into size-bounded *segments*. Each segment is a
flat block of bytes. This is a `[]byte` in memory which may be backed by a file using `mmap(2)`.

As data is inserted, each input row is inserted into the appropriate interval based on its timestamp. The
timestamp is not stored with the data. Within the interval, rows are collapsed together if possible. This
means that if two rows in the same interval have the same value for each dimension, then they are combined
into a single row by summing the values of the metrics.

A segment is composed of many sequential rows. Each row is laid out using 8, 16, 32, or 64-bit slots according
to the type of the column. The initial few bytes of the row contain a bit of metadata.

* The first byte is the *count column*: it indicates how many input rows have been collapsed together to form
  that row.
* After that, there are a few *nil bytes*: this is a bit vector for the dimensions columns in that row that
  indicates whether each column has a nil value. The number of nil bytes can be computed from the number of
  dimension columns in the schema.
* Next come the dimension columns. String-valued columns use a separate dictionary of string to int and so all
  values in the row are numeric.
* Finally, the metric columns are laid out.

Here is a picture of an example row:

```
[00000011][00000010][...d0...][.......d1.......][...d2...][...m0...][...m1...][...m2...]
   count     nil    <-------- dimension columns ---------><------ metric columns ------>
<------------------------------ table.RowSize (16) ------------------------------------>
```

In this example, the schema has 3 dimension columns and 3 metric columns. All the column types are 8 bits
except for d1, a 16-bit type. The count is 3, so three input rows with the same values for the dimension
columns were collapsed together to form this row. There is only one nil byte, and the only set bit is at
position 1, so dimension column 1 (d1) is the only nil column.

Schema Changes
==============

The migrator tool can be used to modify the schema of gumshoedb without losing data. Sample usage:

    make migrator
    ./bin/migrator -old-table db/table -new-table db/new-table -config new_config.toml

Migrator will add columns, delete columns, or increase column sizes. The behavior for decreasing column sizes
(int32 -> int16) is currently undefined.

Notes
=====

There is a subtle bug where we are unable to run gumshoedb in vagrant using a db that is located in a shared
directory on the host machine. See [this][1] stackoverflow question for details.

[1]: http://stackoverflow.com/questions/18420473/invalid-argument-for-read-write-mmap

Contributors
============
* Phil Crosby ([philc](https://github.com/philc))
* Daniel MacDougall ([dmacdougall](https://github.com/dmacdougall))
* Caleb Spare ([cespare](https://github.com/cespare))
