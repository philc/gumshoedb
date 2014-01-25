GumshoeDB
=========

GumshoeDB is a database for quickly scouring hundreds of millions of analytics events and finding
answers. It's a work-in-progress and not ready for consumption. Details about typical use cases and key design
choices are coming soon.

To run:

    make deps
    make run-web

This starts a GumshoeDB daemon at [localhost:9000](http://localhost:9000).

You can interact with GumshoeDB over HTTP. Here's a representative query, assuming the columns "country",
"age", and "clicks".

    curl -XPOST localhost:9000/tables/facts

    {
      table: "events",
      "aggregates":[
          {"type": "sum", "name": "clicks", "column": "clicks"},
          {"type": "avg", "name": "avgAge, "column": "age"}],
      "filters": [{"type": "greaterThan", "column": "age", "value": 21},
                  {"type": "in", "column": "country", "value": ["USA", "Japan"]}],
      "groupings": [{"column": "at", "name":"date", "timeTransform": "day"}]
    }

    Results:
    {
      results:
        [{date: "2013-12-01", country: "Japan", clicks: 123, rowCount: 145},
         {date: "2013-12-01", country: "USA", clicks: 123, rowCount: 145}]
    }

See [DEVELOPING.md](https://github.com/philc/gumshoedb/blob/master/DEVELOPING.md) for how to navigate the code
and make changes.

Gumshoedb is licensed under [the MIT license](http://www.opensource.org/licenses/mit-license.php).

Deploying
=========

To deploy gumshoedb, run:

    ansible-playbook ansible/gumshoedb.yml -i ansible/hosts -e 'hosts=vagrant'

Notes
=====

There is a subtle bug where we are unable to run gumshoedb in vagrant using a db that is located in a shared
directory on the host machine. See [this][1] stackoverflow question for details.

[1]: http://stackoverflow.com/questions/18420473/invalid-argument-for-read-write-mmap
