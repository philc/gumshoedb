package gumshoe

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	mmap "github.com/edsrzf/mmap-go"
)

// TODO(caleb) Increase this (benchmark)
const numRequestGoroutines = 1

// StaticTable is an immutable snapshot of the DB's data.
type StaticTable struct {
	*Schema         `json:"-"`
	Intervals       IntervalMap
	DimensionTables []*DimensionTable // Same length as the number of dimensions; non-string columns are nil
	Count           int               // Number of logical rows
	requests        chan *Request     // The request workers pull from this channel
	wg              *sync.WaitGroup   // For outstanding requests, to know when we can GC this StaticTable
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

// NewStaticTable returns a blank StaticTable corresponding to schema. The StaticTable is not backed by files
// (yet) and has no data.
func NewStaticTable(schema *Schema) *StaticTable {
	staticTable := &StaticTable{
		Schema:          schema,
		Intervals:       make(map[time.Time]*Interval),
		DimensionTables: NewDimensionTablesForSchema(schema),
		requests:        make(chan *Request),
		wg:              new(sync.WaitGroup),
	}
	return staticTable
}

func (s *StaticTable) initialize(schema *Schema) error {
	s.Schema = schema
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

func (s *StaticTable) startRequestWorkers() {
	s.requests = make(chan *Request)
	for i := 0; i < numRequestGoroutines; i++ {
		go s.handleRequests()
	}
}

func (s *StaticTable) stopRequestWorkers() {
	close(s.requests)
}

func (s *StaticTable) handleRequests() {
	done := make(chan struct{})
	for req := range s.requests {
		req.Resp <- &Response{StaticTable: s, done: done}
		<-done
		s.wg.Done()
	}
}

// compressionRatio returns the ratio of logical rows in the table to the stored rows in this StaticTable. For
// instance, if the rows are completely uncollapsible, then the count is 1 for every row and the compression
// ratio is 1. If 4 rows are stored in the table but they all fit into a single collapsed row, then the
// compression factor is 4.0. Note that this function scans the table.
func (s *StaticTable) compressionRatio() float64 {
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

type byTime []time.Time

func (t byTime) Len() int           { return len(t) }
func (t byTime) Less(i, j int) bool { return t[i].Before(t[j]) }
func (t byTime) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }

func (s *StaticTable) sortedIntervalTimes() []time.Time {
	var times []time.Time
	for t := range s.Intervals {
		times = append(times, t)
	}
	sort.Sort(byTime(times))
	return times
}

func (s *StaticTable) debugPrint() {
	fmt.Println("STATE DEBUG ----------------------------------")
	for _, t := range s.sortedIntervalTimes() {
		interval := s.Intervals[t]
		fmt.Printf("Interval [t = %s]\n\n", t)
		for i, segment := range interval.Segments {
			fmt.Printf("  Segment %d\n", i)
			for j := 0; j < len(segment.Bytes); j += s.RowSize {
				fmt.Printf("  % x", segment.Bytes[j:j+countColumnWidth])
				dimColumnStartOffset := j + s.DimensionStartOffset + s.NilBytes
				fmt.Printf(" ][ % x", segment.Bytes[j+s.DimensionStartOffset:dimColumnStartOffset])
				fmt.Printf(" | % x", segment.Bytes[dimColumnStartOffset:j+s.MetricStartOffset])
				fmt.Printf(" ][ % x\n", segment.Bytes[j+s.MetricStartOffset:j+s.RowSize])
			}
			fmt.Println()
		}
	}
	fmt.Println("----------------------------------------------")
}
