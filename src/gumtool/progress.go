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

func (p *Progress) Clear() {
	p.Lock()
	defer p.Unlock()
	fmt.Print("\r")
}

func (p *Progress) Print() {
	p.Lock()
	defer p.Unlock()
	p.print()
}

func (p *Progress) print() {
	fmt.Printf("\r%s: %6d/%d (%.1f%%)", p.Tag, p.N, p.Total, float64(p.N)/float64(p.Total)*100)
}

func (p *Progress) Add(delta int) {
	p.Lock()
	defer p.Unlock()
	p.N += delta
	p.print()
}
