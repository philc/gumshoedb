# asrt

A very lightweight assertions package for Go.

## To Do

- Better failure message for `Equal` and `DeepEqual` when `got` or `want` are large. It's very hard to see the
  difference between, say, two 200-element slices. A nice structural diff implementation would go a very long
  way.
