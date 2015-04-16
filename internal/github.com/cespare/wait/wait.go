// Package wait provides Group, an extended version of sync.WaitGroup.
package wait

import "sync"

// A Group waits for a collection of goroutines to exit.
// With Wait, one may wait for all goroutines to exit and
// get the first error that was seen.
// Cooperative cancellation is also supported via Quit
// and the broadcast chan passed to each goroutine.
// The zero value of Group is ready to be used.
type Group struct {
	wg     sync.WaitGroup
	mu     sync.Mutex
	quit   chan struct{}
	closed bool
	err    error
}

// Go runs f in a new goroutine.
// The quit chan is closed to indicate that f should exit early,
// so f is expected to periodically receive from quit
// and immediately return nil if a value arrives.
// Quit is called automatically if f returns a non-nil error.
func (g *Group) Go(f func(quit <-chan struct{}) error) {
	g.mu.Lock()
	if g.quit == nil {
		g.quit = make(chan struct{})
	}
	g.mu.Unlock()

	g.wg.Add(1)
	go func() {
		err := f(g.quit)
		if err != nil {
			g.mu.Lock()
			if g.err == nil {
				g.err = err
			}
			g.mu.Unlock()
			g.Quit()
		}
		g.wg.Done()
	}()
}

// Quit asks all goroutines started with Go to exit
// (by closing the quit chan given to each function).
// After the first call to Quit, subsequent calls have no effect.
func (g *Group) Quit() {
	g.mu.Lock()
	if !g.closed && g.quit != nil {
		close(g.quit)
		g.closed = true
	}
	g.mu.Unlock()
}

// Wait waits for all goroutines to exit and returns
// the first non-nil error seen.
func (g *Group) Wait() error {
	g.wg.Wait()
	return g.err
}
