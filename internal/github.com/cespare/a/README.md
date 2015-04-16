# a

Package `a` is a Go assertions library for tests. It is very similar to [gocheck](http://labix.org/gocheck)'s
`Checker`s, with some inspiration from [testify/assert](https://github.com/stretchr/testify).

The flavor:

``` go
func TestFoo(t *testing.T) {
  foo, err := Foo()
  a.Assert(t, err, a.IsNil) // t.Fatal
  a.Check(t, foo, a.Equals, 3, "foo was expected to be 3") // t.Error; custom message
}
```

**Status:** alpha. Not documented yet. Still WIP.
