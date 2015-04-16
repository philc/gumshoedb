package gostc

import (
	"io/ioutil"
	"math/rand"
	"testing"
	"time"
)

type nopWriteCloser struct{}

func (nopWriteCloser) Write(b []byte) (int, error) {
	return ioutil.Discard.Write(b)
}

func (nopWriteCloser) Close() error {
	return nil
}

var devNull = nopWriteCloser{}

func NewBenchClient() *Client {
	c, err := NewClient("localhost:12345")
	if err != nil {
		panic(err)
	}
	c.c = devNull
	return c
}

func BenchmarkCount(b *testing.B) {
	c := NewBenchClient()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Count("foo.bar", 123, 0.5)
	}
}

func BenchmarkInc(b *testing.B) {
	c := NewBenchClient()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Inc("foo.bar")
	}
}

func BenchmarkTime(b *testing.B) {
	c := NewBenchClient()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Time("foo.bar", time.Second)
	}
}

func BenchmarkGauge(b *testing.B) {
	c := NewBenchClient()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Gauge("foo.bar", 123.456)
	}
}

func BenchmarkSet(b *testing.B) {
	c := NewBenchClient()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set("foo.bar", []byte("hello world"))
	}
}

func BenchmarkCountProb(b *testing.B) {
	rand.Seed(0)
	c := NewBenchClient()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.CountProb("foo.bar", 123, 0.1)
	}
}

func BenchmarkIncProb(b *testing.B) {
	rand.Seed(0)
	c := NewBenchClient()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.IncProb("foo.bar", 0.1)
	}
}
