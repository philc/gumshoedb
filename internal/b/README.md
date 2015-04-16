This package was generated from github.com/cznic/b. After cloning the repo:

    $ make generic | sed -e 's/KEY/[]byte/g' -e 's/VALUE/MetricWithCount/g' > path/to/gumshoedb/b/btree.go

I defined the `MetricWithCount` type in `value.go` separately.
