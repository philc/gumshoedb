# gostc

[![GoDoc](https://godoc.org/github.com/cespare/gostc?status.svg)](https://godoc.org/github.com/cespare/gostc)

gostc is a Go [StatsD](https://github.com/etsy/statsd/)/[gost](https://github.com/cespare/gost/) client.

## Installation

    go get github.com/cespare/gostc

## Usage

Quick example:

``` go
client, err := gostc.NewClient("localhost:8125")
if err != nil {
  panic(err)
}

// Users will typically ignore the return errors of gostc methods as statsd
// is a best-effort service in most software.
client.Count("foobar", 1, 1)
client.Inc("foobar") // Same as above
t := time.Now()
time.Sleep(time.Second)
client.Time("blah", time.Since(t))
```
