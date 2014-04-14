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

// StaticTable is an immutable snapshot of the DB's data.
type StaticTable struct {
	*Schema          `json:"-"`
	Intervals        IntervalMap
	DimensionTables  []*DimensionTable // Same length as the number of dimensions; non-string columns are nil
	Count            int               // Number of logical rows
	requests         chan *Request     // The request workers pull from this channel
	querySegmentJobs chan func()       // A copy of the chan in the DB
	wg               *sync.WaitGroup   // For outstanding requests, to know when we can GC this StaticTable
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

	// Load dimension tables
	for i, col := range schema.DimensionColumns {
		if !col.String {
			continue
		}
		if err := s.DimensionTables[i].Load(schema, i); err != nil {
			return err
		}
	}

	// Load each interval/segment
	for _, interval := range s.Intervals {
		interval.Segments = make([]*Segment, interval.NumSegments)
		for i := 0; i < interval.NumSegments; i++ {

			f, err := os.Open(interval.SegmentFilename(schema, i))
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

// numRequestWorkers controls the number of workers servicing requests for a StaticTable.
const numRequestWorkers = 16

func (s *StaticTable) startRequestWorkers() {
	s.requests = make(chan *Request)
	for i := 0; i < numRequestWorkers; i++ {
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

type StaticTableStats struct {
	Intervals int
	Segments  int
	Rows      int
	Bytes     int

	// CompressionRatio is the ratio of logical rows in the table to the stored rows in this StaticTable. For
	// instance, if the rows are completely uncollapsible, then the count is 1 for every row and the compression
	// ratio is 1. If 4 rows are stored in the table but they all fit into a single collapsed row, then the
	// compression factor is 4.0.
	CompressionRatio float64

	ByInterval map[time.Time]*IntervalStats
}

type IntervalStats struct {
	Segments int
	Rows     int
	Bytes    int
}

// stats does a full table scan and returns various metrics about the table in the form of StaticTableStats.
func (s *StaticTable) stats() *StaticTableStats {
	stats := &StaticTableStats{
		Intervals:  len(s.Intervals),
		ByInterval: make(map[time.Time]*IntervalStats),
	}

	logicalRows := 0
	physicalRows := 0
	for t, interval := range s.Intervals {
		stats.Segments += interval.NumSegments
		physicalRows += interval.NumRows
		intervalBytes := 0
		for _, segment := range interval.Segments {
			intervalBytes += len(segment.Bytes)
			for cursor := 0; cursor < len(segment.Bytes); cursor += s.RowSize {
				row := RowBytes(segment.Bytes[cursor : cursor+s.RowSize])
				logicalRows += int(row.count(s.Schema))
			}
		}
		stats.Bytes += intervalBytes
		stats.ByInterval[t] = &IntervalStats{interval.NumSegments, interval.NumRows, intervalBytes}
	}

	stats.Rows = physicalRows
	stats.CompressionRatio = float64(logicalRows) / float64(physicalRows)
	return stats
}
