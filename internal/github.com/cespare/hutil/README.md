# hutil is deprecated. See github.com/cespare/go-apachelog

## hutil

hutil is a package of utilities for Go `net/http` servers.

**Status:** Work in progress. Most tests and documentations are yet to come. Use at your own risk.

## Functions

* `RemoteIP`: Logic for determining the best-guess remote IP address for the end client, given an
  `http.Request`.

## Subpackages

* **apachelog**: An easy-to-use logger for HTTP requests that imitates the Apache Common Log Format.
