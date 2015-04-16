package gostc

import (
	"net"
	"testing"
	"time"

	"github.com/philc/gumshoedb/internal/github.com/cespare/asrt"
)

type TestServer struct {
	Addr     string
	Conn     *net.UDPConn
	Messages chan []byte
}

func NewTestServer() *TestServer {
	s := &TestServer{}
	u, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	conn, err := net.ListenUDP("udp", u)
	if err != nil {
		panic(err)
	}
	s.Conn = conn
	s.Addr = conn.LocalAddr().String()
	s.Messages = make(chan []byte)
	go func() {
		for {
			buf := make([]byte, 1000)
			n, _, err := s.Conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			s.Messages <- buf[:n]
		}
	}()
	return s
}

func (s *TestServer) Close() {
	s.Conn.Close()
}

func (s *TestServer) NextMessage() string {
	return string(<-s.Messages)
}

func MakeServerAndClient() (*TestServer, *Client) {
	s := NewTestServer()
	c, err := NewClient(s.Addr)
	if err != nil {
		panic(err)
	}
	return s, c
}

func MakeNonRandom(seq []float64) func() float64 {
	i := 0
	return func() float64 {
		v := seq[i]
		i++
		if i >= len(seq) {
			i = 0
		}
		return v
	}
}

func TestCount(t *testing.T) {
	server, client := MakeServerAndClient()
	defer server.Close()

	client.Count("foo", 3, 1)
	asrt.Equal(t, server.NextMessage(), "foo:3|c")

	client.Count("foo", 3, 0.5)
	asrt.Equal(t, server.NextMessage(), "foo:3|c@0.5")

	client.Count("blah", -123.456, 1)
	asrt.Equal(t, server.NextMessage(), "blah:-123.456|c")

	client.Inc("incme")
	asrt.Equal(t, server.NextMessage(), "incme:1|c")

	randFloat = MakeNonRandom([]float64{0.6, 0.4})
	client.CountProb("foo", 3, 0.5) // nothin
	client.CountProb("bar", 3, 0.5)
	asrt.Equal(t, server.NextMessage(), "bar:3|c@0.5")

	client.IncProb("foo", 0.5)
	client.IncProb("bar", 0.5)
	asrt.Equal(t, server.NextMessage(), "bar:1|c@0.5")
}

func TestTime(t *testing.T) {
	server, client := MakeServerAndClient()
	defer server.Close()

	client.Time("foo", 3*time.Second)
	asrt.Equal(t, server.NextMessage(), "foo:3000|ms")
}

func TestGauge(t *testing.T) {
	server, client := MakeServerAndClient()
	defer server.Close()

	client.Gauge("foo", 123.456)
	asrt.Equal(t, server.NextMessage(), "foo:123.456|g")
}

func TestSet(t *testing.T) {
	server, client := MakeServerAndClient()
	defer server.Close()

	client.Set("foo", []byte("hello"))
	asrt.Equal(t, server.NextMessage(), "foo:hello|s")
}

func TestBufferedMaxSize(t *testing.T) {
	s := NewTestServer()
	// 5 ms is hopefully enough time to be processed. Kind of a fragile test, but simple.
	c, err := NewBufferedClient(s.Addr, 100, 12, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	for i := byte(0); i < 4; i++ {
		c.Set("a", []byte{'a' + i})
	}
	c.Close()
	asrt.Equal(t, s.NextMessage(), "a:a|s\na:b|s")
	asrt.Equal(t, s.NextMessage(), "a:c|s\na:d|s")
}

func TestBufferedMinFlush(t *testing.T) {
	s := NewTestServer()
	c, err := NewBufferedClient(s.Addr, 100, 100, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	for i := byte(0); i < 4; i++ {
		c.Set("a", []byte{'a' + i})
		if i == 1 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	c.Close()
	asrt.Equal(t, s.NextMessage(), "a:a|s\na:b|s")
	asrt.Equal(t, s.NextMessage(), "a:c|s\na:d|s")
}
