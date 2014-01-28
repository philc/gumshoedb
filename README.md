GumshoeDB
=========

GumshoeDB is a database for quickly scouring hundreds of millions of analytics events and finding
answers. It's a work-in-progress and not ready for consumption. Details about typical use cases and key design
choices are coming soon.

To run:

    make deps
    make run-web

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
      "table": "events",
      "aggregates":[
          {"type": "sum", "name": "clicks", "column": "clicks"},
          {"type": "average", "name": "avgAge", "column": "age"}],
      "filters": [{"type": "greaterThan", "column": "age", "value": 20},
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

Deploying
=========

To deploy gumshoedb, run:

    ansible-playbook ansible/gumshoedb.yml -i ansible/hosts -e 'hosts=vagrant'

Notes
=====

There is a subtle bug where we are unable to run gumshoedb in vagrant using a db that is located in a shared
directory on the host machine. See [this][1] stackoverflow question for details.

[1]: http://stackoverflow.com/questions/18420473/invalid-argument-for-read-write-mmap
