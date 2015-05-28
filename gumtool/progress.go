package main

import (
	"fmt"
	"sync"
)

// Progress is used for printing an updating progress meter for a task.
type Progress struct {
	sync.Mutex
	Tag   string
	N     int
	Total int
}

func NewProgress(tag string, total int) *Progress { return &Progress{Tag: tag, Total: total} }

func (p *Progress) Print() {
	p.Lock()
	defer p.Unlock()
	p.print()
}

func (p *Progress) print() {
	fmt.Printf("%s: %6d/%d (%.1f%%)\n", p.Tag, p.N, p.Total, percent(p.N, p.Total))
}

func (p *Progress) Add(delta int) {
	p.Lock()
	defer p.Unlock()
	p1 := percent(p.N, p.Total)
	p.N += delta
	p2 := percent(p.N, p.Total)
	// only print every 5%
	if int(p1)/5 < int(p2)/5 {
		p.print()
	}
}

func percent(n, d int) float64 {
	return float64(n) / float64(d) * 100
}
