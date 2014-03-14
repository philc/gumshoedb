package gumshoe

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"
	"unsafe"

	mmap "github.com/edsrzf/mmap-go"
)

// TODO(caleb): Move this constant to a logical location
const intervalDuration = time.Hour

// A Segment is an immutable chunk of memory that is part of the data in an interval. It may be backed by a
// memory-mapped file.
type Segment struct {
	File  *os.File // Nil if this segment is not backed by a file
	Bytes mmap.MMap
}

type Interval struct {
	Generation  int        // An incrementing sequence number
	Start       time.Time  // Inclusive
	End         time.Time  // Exclusive
	Segments    []*Segment `json:"-"`
	NumSegments int        // Maintained separately for JSON encoding
	NumRows     int
}

// An intervalCursor holds the necessary state to iterate through all the keys of an Interval, in order,
// one-at-a-time. (That is, even if the same key appears multiple times in a row because the count is > 256,
// it will appear once in the enumeration.) Note that this is iteration approach is not intended to be
// efficient enough for queries.
type intervalCursor struct {
	*Schema
	Interval     *Interval
	SegmentIndex int
	Offset       int
}

func (iv *Interval) cursor(s *Schema) *intervalCursor {
	return &intervalCursor{
		Schema:   s,
		Interval: iv,
	}
}

// Next reads forward throught the Interval and returns the next key/val pair with count. ok indicates whether
// iteration should stop.
func (ic *intervalCursor) Next() (key, val []byte, count int, ok bool) {
	if ic.SegmentIndex >= len(ic.Interval.Segments) {
		return
	}
	segment := ic.Interval.Segments[ic.SegmentIndex]
	if ic.Offset >= len(segment.Bytes) {
		ic.Offset = 0
		ic.SegmentIndex++
		return ic.Next()
	}

	key = segment.Bytes[ic.Offset+ic.DimensionStartOffset : ic.Offset+ic.MetricStartOffset]
	val = segment.Bytes[ic.Offset+ic.MetricStartOffset : ic.Offset+ic.RowSize]
	count = int(*(*uint32)(unsafe.Pointer(&segment.Bytes[ic.Offset])))
	ic.Offset += ic.RowSize
	return key, val, count, true
}

// A writeOnlyInterval is a fresh interval corresponding with write-only segment files which is being filled
// in. After it has been fully written it may be converted to an immutable read-only Interval by calling
// ToInterval.
type writeOnlyInterval struct {
	Generation     int
	Start          time.Time
	End            time.Time
	Segments       int
	CurSegmentFile *os.File
	CurSegmentSize int
	NumRows        int
}

func newWriteOnlyInterval(generation int, start, end time.Time) *writeOnlyInterval {
	return &writeOnlyInterval{
		Generation: generation,
		Start:      start,
		End:        end,
	}
}

func (iv *writeOnlyInterval) writeKeyValCount(key, val []byte, count uint32) error {
	f := iv.CurSegmentFile
	countBytes := make([]byte, countColumnWidth)
	*(*uint32)(unsafe.Pointer(&countBytes[0])) = count
	if _, err := f.Write(countBytes); err != nil {
		return err
	}
	if _, err := f.Write(key); err != nil {
		return err
	}
	if _, err := f.Write(val); err != nil {
		return err
	}
	iv.NumRows++
	return nil
}

// appendRow appends a new row with count to interval. (It writes multiple rows if the count is too large to
// represent directly). Rows must be inserted in increasing key (dimension) order, with one call for each row
// of a given key.
func (iv *writeOnlyInterval) appendRow(s *Schema, dimensions, metrics []byte, count int) error {
	if iv.CurSegmentFile == nil || iv.CurSegmentSize > s.SegmentSize {
		// Need to open a new segment after closing the current one, if it exists.
		if iv.CurSegmentFile != nil {
			if err := iv.CurSegmentFile.Close(); err != nil {
				return err
			}
		}
		filename := s.SegmentFilename(iv.Start, iv.Generation, iv.Segments)
		iv.Segments++
		f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
		if err != nil {
			return err
		}
		iv.CurSegmentFile = f
	}
	if count > math.MaxUint32 {
		panic("count greater than MaxUint32 is unrepresentable with uint32 for column count")
	}
	return iv.writeKeyValCount(dimensions, metrics, uint32(count))
}

func (iv *writeOnlyInterval) freeze(s *Schema) (*Interval, error) {
	if iv.CurSegmentFile != nil {
		if err := iv.CurSegmentFile.Close(); err != nil {
			return nil, err
		}
	}

	segments := make([]*Segment, iv.Segments)
	for i := 0; i < iv.Segments; i++ {
		filename := s.SegmentFilename(iv.Start, iv.Generation, i)
		f, err := os.Open(filename)
		if err != nil {
			return nil, err
		}
		mapped, err := mmap.Map(f, mmap.RDONLY, 0)
		if err != nil {
			return nil, err
		}
		segments[i] = &Segment{File: f, Bytes: mapped}
	}
	return &Interval{
		Generation:  iv.Generation,
		Start:       iv.Start,
		End:         iv.End,
		Segments:    segments,
		NumSegments: len(segments),
		NumRows:     iv.NumRows,
	}, nil
}

func (s *Schema) SegmentFilename(start time.Time, generation, segmentIndex int) string {
	name := fmt.Sprintf("interval.%d.generation%04d.segment%04d", start.Unix(), generation, segmentIndex)
	return filepath.Join(s.Dir, name)
}

// WriteMemInterval writes out the data in memInterval to a fresh Interval with generation 0. Note that no
// interval with this start time should exist.
// TODO(caleb) param: diskBacked bool
func (s *Schema) WriteMemInterval(memInterval *MemInterval) (*Interval, error) {
	cursor, err := memInterval.Tree.SeekFirst()
	if err != nil {
		return nil, err
	}
	interval := newWriteOnlyInterval(0, memInterval.Start, memInterval.End)
	for {
		key, val, err := cursor.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if err := interval.appendRow(s, key, val.Metric, val.Count); err != nil {
			return nil, err
		}
	}
	return interval.freeze(s)
}

// WriteCombinedInterval writes out the combined data from memInterval and stateInterval to a fresh Interval
// with generation stateInterval.Generation+1.
// TODO(caleb) param: diskBacked bool
func (s *Schema) WriteCombinedInterval(memInterval *MemInterval, stateInterval *Interval) (*Interval, error) {
	// Sanity check
	if memInterval.Start != stateInterval.Start || memInterval.End != stateInterval.End {
		panic("attempt to combine memInterval/stateInterval from different times")
	}

	memCursor, err := memInterval.Tree.SeekFirst()
	if err != nil {
		return nil, err
	}
	stateCursor := stateInterval.cursor(s)
	interval := newWriteOnlyInterval(stateInterval.Generation+1, memInterval.Start, memInterval.End)

	// Do an initial read from both mem and state, then loop and compare, advancing one or both (a classic
	// merge).
	moreMemKeys := true
	moreStateKeys := true
	memKey, memVal, err := memCursor.Next()
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
		moreMemKeys = false
	}
	stateKey, stateVal, stateCount, ok := stateCursor.Next()
	if !ok {
		moreStateKeys = false
	}

	for moreMemKeys && moreStateKeys {
		cmp := bytes.Compare(memKey, stateKey)
		var advanceMem, advanceState bool
		switch {
		case cmp < 0:
			if err := interval.appendRow(s, memKey, memVal.Metric, memVal.Count); err != nil {
				return nil, err
			}
			advanceMem = true
		case cmp > 0:
			if err := interval.appendRow(s, stateKey, stateVal, stateCount); err != nil {
				return nil, err
			}
			advanceState = true
		default: // equal
			metrics := make(MetricBytes, len(memVal.Metric))
			copy(metrics, memVal.Metric)
			metrics.add(s, MetricBytes(stateVal))
			count := memVal.Count + stateCount
			if err := interval.appendRow(s, memKey, metrics, count); err != nil {
				return nil, err
			}
			advanceMem = true
			advanceState = true
		}

		if advanceMem {
			memKey, memVal, err = memCursor.Next()
			if err != nil {
				if err != io.EOF {
					return nil, err
				}
				moreMemKeys = false
			}
		}
		if advanceState {
			stateKey, stateVal, stateCount, ok = stateCursor.Next()
			if !ok {
				moreStateKeys = false
			}
		}
	}

	// Possibly drain leftover key/vals from memInterval or stateInterval.

	if moreMemKeys {
		for {
			key, val, err := memCursor.Next()
			if err != nil {
				if err != io.EOF {
					return nil, err
				}
				break
			}
			if err := interval.appendRow(s, key, val.Metric, val.Count); err != nil {
				return nil, err
			}
		}
	}
	if moreStateKeys {
		for {
			key, val, count, ok := stateCursor.Next()
			if !ok {
				break
			}
			if err := interval.appendRow(s, key, val, count); err != nil {
				return nil, err
			}
		}
	}

	return interval.freeze(s)
}
