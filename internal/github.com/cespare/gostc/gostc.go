// Package gostc implemenents a StatsD/gost client.
//
// This package performs minimal input validation, leaving that to the gost server.
package gostc

import (
	"errors"
	"io"
	"math/rand"
	"net"
	"strconv"
	"time"
)

// A Client is a StatsD/gost client which has a UDP connection.
type Client struct {
	c        io.WriteCloser
	buffered bool
	// These are only are used for buffered clients
	incoming chan []byte
	quit     chan chan bool
}

// NewClient creates a client with the given UDP address.
func NewClient(addr string) (*Client, error) {
	u, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	c, err := net.DialUDP("udp", nil, u)
	if err != nil {
		return nil, err
	}

	client := &Client{
		c: c,
	}
	return client, nil
}

// NewBufferedClient creates a client with the given UDP address that can buffer messages and sends them
// together in batches (separated by newlines, per the statsd protocol). Messages are formatted and sent to a
// single sending goroutine via a buffered channel. This has the effect of offloading the CPU and clock time
// of sending the messages from the calling goroutine, as well as possibly increasing efficiency by reducing
// the volume of UDP packets sent.
//
// A buffered Client may or may not change (improve, degrade) performance in your particular scenario. Default
// to using a normal client (via NewClient) unless gostc performance is a measurable bottleneck, and then see
// if a buffered client helps (and keep measuring).
//
// The three parameters queueSize, maxPacketBytes, and minFlush tune the buffered channel size, maximum single
// packet size, and minimum time between flushes. Message are buffered until maxPacketBytes is reached or
// until some time as passed (no more than minFlush). Use NewDefaultBufferedClient for reasonable defaults.
//
// Note that a buffered client cannot report UDP errors (it will silently fail).
func NewBufferedClient(addr string, queueSize int, maxPacketBytes int, minFlush time.Duration) (*Client, error) {
	c, err := NewClient(addr)
	if err != nil {
		return nil, err
	}
	c.buffered = true
	c.incoming = make(chan []byte, queueSize)
	c.quit = make(chan chan bool)
	go c.bufferAndSend(maxPacketBytes, minFlush)
	return c, nil
}

const (
	// 100 * DefaultMaxPacketBytes = 10KB, as a lower bound on memory usage.
	DefaultQueueSize = 100
	// 1/10th of gost's default max, and 1k packets seem to generally work for Linux local UDP.
	DefaultMaxPacketBytes = 1000
	DefaultMinFlush       = time.Second
)

// NewDefaultBufferedClient calls NewBufferedClient with tuning parameters queueSize, maxPacketBytes, and
// minFlush set to DefaultQueueSize, DefaultMaxPacketBytes, and DefaultMinFlush, respectively.
func NewDefaultBufferedClient(addr string) (*Client, error) {
	return NewBufferedClient(addr, DefaultQueueSize, DefaultMaxPacketBytes, DefaultMinFlush)
}

func (c *Client) send(b []byte) error {
	_, err := c.c.Write(b)
	return err
}

func (c *Client) bufferAndSend(maxPacketBytes int, minFlush time.Duration) {
	timer := time.NewTimer(minFlush)
	buf := make([]byte, 0, maxPacketBytes)
	for {
		select {
		case <-timer.C:
			if len(buf) > 0 {
				c.send(buf)
				buf = buf[:0]
			}
			timer.Reset(minFlush)
		case msg := <-c.incoming:
			if len(buf)+len(msg)+1 > maxPacketBytes {
				c.send(buf)
				buf = buf[:0]
				timer.Reset(minFlush)
			}
			if len(buf) > 0 {
				buf = append(buf, '\n')
			}
			buf = append(buf, msg...)
		case q := <-c.quit:
			// Drain incoming
			for msg := range c.incoming {
				if len(buf)+len(msg)+1 > maxPacketBytes {
					c.send(buf)
					buf = buf[:0]
				}
				if len(buf) > 0 {
					buf = append(buf, '\n')
				}
				buf = append(buf, msg...)
			}
			if len(buf) > 0 {
				c.send(buf)
			}
			q <- true
			return
		}
	}
}

// Close closes the client's UDP connection. Afterwards, the client cannot be used. If the client is buffered,
// Close first sends any buffered messages.
func (c *Client) Close() error {
	if c.buffered {
		q := make(chan bool)
		c.quit <- q
		close(c.incoming)
		<-q
	}
	return c.c.Close()
}

// ErrSamplingRate is returned by client.Count (or variants) when a bad sampling rate value is provided.
var ErrSamplingRate = errors.New("sampling rate must be in (0, 1]")

// Count submits a statsd count message with the given key, value, and sampling rate.
func (c *Client) Count(key string, delta, samplingRate float64) error {
	msg := []byte(key)
	msg = append(msg, ':')
	msg = strconv.AppendFloat(msg, delta, 'f', -1, 64)
	msg = append(msg, []byte("|c")...)
	switch {
	case samplingRate > 1 || samplingRate <= 0:
		return ErrSamplingRate
	case samplingRate == 1:
	default:
		msg = append(msg, '@')
		msg = strconv.AppendFloat(msg, samplingRate, 'f', -1, 64)
	}
	if c.buffered {
		c.incoming <- msg
		return nil
	}
	return c.send(msg)
}

// inc does count(key, 1, samplingRate). strconv's float formatting is actually quite (relatively) slow, so
// special-casing 1 makes inc a lot faster than the more general Count.
func (c *Client) inc(key string, p float64) error {
	msg := make([]byte, len(key), len(key)+4)
	copy(msg, key)
	msg = append(msg, []byte(":1|c")...)
	if p != 1 {
		msg = append(msg, '@')
		msg = strconv.AppendFloat(msg, p, 'f', -1, 64)
	}
	if c.buffered {
		c.incoming <- msg
		return nil
	}
	return c.send(msg)
}

// Inc submits a count with delta and sampling rate equal to 1.
func (c *Client) Inc(key string) error {
	return c.inc(key, 1)
}

var randFloat = rand.Float64

// CountProb counts (key, delta) with probability p in (0, 1].
func (c *Client) CountProb(key string, delta, p float64) error {
	if p > 1 || p <= 0 {
		return ErrSamplingRate
	}
	if randFloat() >= p {
		return nil
	}
	return c.Count(key, delta, p)
}

// IncProb increments key with probability p in (0, 1].
func (c *Client) IncProb(key string, p float64) error {
	if p > 1 || p <= 0 {
		return ErrSamplingRate
	}
	if randFloat() >= p {
		return nil
	}
	return c.inc(key, p)
}

// Time submits a statsd timer message.
func (c *Client) Time(key string, duration time.Duration) error {
	msg := []byte(key)
	msg = append(msg, ':')
	msg = strconv.AppendFloat(msg, duration.Seconds()*1000, 'f', -1, 64)
	msg = append(msg, []byte("|ms")...)
	if c.buffered {
		c.incoming <- msg
		return nil
	}
	return c.send(msg)
}

// Gauge submits a statsd gauge message.
func (c *Client) Gauge(key string, value float64) error {
	msg := []byte(key)
	msg = append(msg, ':')
	msg = strconv.AppendFloat(msg, value, 'f', -1, 64)
	msg = append(msg, []byte("|g")...)
	if c.buffered {
		c.incoming <- msg
		return nil
	}
	return c.send(msg)
}

// Set submits a statsd set message.
func (c *Client) Set(key string, element []byte) error {
	msg := make([]byte, len(key), len(key)+1+len(element)+2)
	copy(msg, key)
	msg = append(msg, ':')
	msg = append(msg, element...)
	msg = append(msg, []byte("|s")...)
	if c.buffered {
		c.incoming <- msg
		return nil
	}
	return c.send(msg)
}
