package gumshoe

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	mmap "github.com/edsrzf/mmap-go"
)

// TODO(caleb) Increase this (benchmark)
const numRequestGoroutines = 1

// State is an immutable snapshot of the DB's data.
type State struct {
	*Schema         `json:"-"`
	Intervals       IntervalMap
	DimensionTables []*DimensionTable // Same length as the number of dimensions; non-string columns are nil
	Count           int               // Number of logical rows
	requests        chan *Request     // The request workers pull from this channel
	wg              *sync.WaitGroup   // For outstanding requests, so that we can know when we can GC this state
}

// IntervalMap is a type that implements JSON conversions for map[time.Time]*Interval. (This doesn't work
// normally because encoding/json does not encode map keys -- they must be strings).
type IntervalMap map[time.Time]*Interval

type intervalByTime struct {
	Time     time.Time
	Interval *Interval
}

func (m IntervalMap) MarshalJSON() ([]byte, error) {
	var intervals []*intervalByTime
	for t, interval := range m {
		intervals = append(intervals, &intervalByTime{Time: t, Interval: interval})
	}
	return json.Marshal(intervals)
}

func (m *IntervalMap) UnmarshalJSON(b []byte) error {
	if *m == nil {
		im := make(IntervalMap)
		*m = im
	}
	var intervals []*intervalByTime
	if err := json.Unmarshal(b, &intervals); err != nil {
		return err
	}
	for _, intervalAndTime := range intervals {
		(*m)[intervalAndTime.Time] = intervalAndTime.Interval
	}
	return nil
}

// NewState returns a blank state corresponding to schema. The state is not backed by files (yet) and has no
// data.
func NewState(schema *Schema) *State {
	state := &State{
		Schema:          schema,
		Intervals:       make(map[time.Time]*Interval),
		DimensionTables: NewDimensionTablesForSchema(schema),
		requests:        make(chan *Request),
		wg:              new(sync.WaitGroup),
	}
	return state
}

func (s *State) initialize(schema *Schema) error {
	s.Schema = schema
	s.requests = make(chan *Request)
	s.wg = new(sync.WaitGroup)

	for _, interval := range s.Intervals {
		interval.Segments = make([]*Segment, interval.NumSegments)
		for i := 0; i < interval.NumSegments; i++ {

			f, err := os.Open(schema.SegmentFilename(interval.Start, interval.Generation, i))
			if err != nil {
				return err
			}
			mapped, err := mmap.Map(f, mmap.RDONLY, 0)
			if err != nil {
				return err
			}
			interval.Segments[i] = &Segment{File: f, Bytes: mapped}
		}
	}
	return nil
}

func (s *State) spinUpRequestWorkers() {
	for i := 0; i < numRequestGoroutines; i++ {
		go s.handleRequests()
	}
}

func (s *State) handleRequests() {
	done := make(chan struct{})
	for req := range s.requests {
		req.Resp <- &Response{State: s, done: done}
		<-done
		s.wg.Done()
	}
}

// compressionRatio returns the ratio of logical rows in the table to the stored rows in this state. For
// instance, if the rows are completely uncollapsible, then the count is 1 for every row and the compression
// ratio is 1. If 4 rows are stored in the table but they all fit into a single collapsed row, then the
// compression factor is 4.0. Note that this function scans the table.
func (s *State) compressionRatio() float64 {
	logicalRows := 0
	physicalRows := 0
	for _, interval := range s.Intervals {
		physicalRows += interval.NumRows
		for _, segment := range interval.Segments {
			for cursor := 0; cursor < len(segment.Bytes); cursor += s.RowSize {
				row := RowBytes(segment.Bytes[cursor : cursor+s.RowSize])
				logicalRows += int(row.count(s.Schema))
			}
		}
	}
	return float64(logicalRows) / float64(physicalRows)
}
