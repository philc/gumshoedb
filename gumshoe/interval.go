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

	mmap "github.com/philc/gumshoedb/internal/github.com/edsrzf/mmap-go"
)

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
func (ic *intervalCursor) Next() (key, val []byte, count int, more bool) {
	if ic.SegmentIndex >= len(ic.Interval.Segments) {
		return nil, nil, 0, false
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
// freeze.
type writeOnlyInterval struct {
	Interval
	DiskBacked     bool
	CurSegment     io.Writer
	CurSegmentSize int
	buffers        []*bytes.Buffer // Used if !DiskBacked
}

func newWriteOnlyInterval(diskBacked bool, generation int, start, end time.Time) *writeOnlyInterval {
	return &writeOnlyInterval{
		Interval: Interval{
			Generation: generation,
			Start:      start,
			End:        end,
		},
		DiskBacked: diskBacked,
	}
}

func (iv *writeOnlyInterval) writeKeyValCount(key, val []byte, count uint32) error {
	countBytes := make([]byte, countColumnWidth)
	*(*uint32)(unsafe.Pointer(&countBytes[0])) = count
	if _, err := iv.CurSegment.Write(countBytes); err != nil {
		return err
	}
	if _, err := iv.CurSegment.Write(key); err != nil {
		return err
	}
	if _, err := iv.CurSegment.Write(val); err != nil {
		return err
	}
	iv.NumRows++
	return nil
}

// appendRow appends a new row with count to interval. (It writes multiple rows if the count is too large to
// represent directly). Rows must be inserted in increasing key (dimension) order, with one call for each row
// of a given key.
func (iv *writeOnlyInterval) appendRow(s *Schema, dimensions, metrics []byte, count int) error {
	if iv.CurSegmentSize+s.RowSize > s.SegmentSize {
		if err := iv.closeCurrentSegment(); err != nil {
			return err
		}
		iv.CurSegmentSize = 0
		iv.CurSegment = nil
	}

	if iv.CurSegment == nil {
		if err := iv.openFreshSegment(s); err != nil {
			return err
		}
	}
	if count > math.MaxUint32 {
		panic("count greater than MaxUint32 is unrepresentable with uint32 for column count")
	}
	iv.CurSegmentSize += s.RowSize
	return iv.writeKeyValCount(dimensions, metrics, uint32(count))
}

// freeze opens segment files as readonly mmaps and returns an immutable *Interval. iv should not be used
// after calling freeze.
func (iv *writeOnlyInterval) freeze(s *Schema) (*Interval, error) {
	if err := iv.closeCurrentSegment(); err != nil {
		return nil, err
	}

	iv.Segments = make([]*Segment, iv.NumSegments)
	for i := 0; i < iv.NumSegments; i++ {
		if !iv.DiskBacked {
			iv.Segments[i] = &Segment{Bytes: iv.buffers[i].Bytes()}
			continue
		}
		filename := iv.SegmentFilename(s, i)
		f, err := os.Open(filename)
		if err != nil {
			return nil, err
		}
		mapped, err := mmap.Map(f, mmap.RDONLY, 0)
		if err != nil {
			return nil, err
		}
		iv.Segments[i] = &Segment{File: f, Bytes: mapped}
	}
	return &iv.Interval, nil
}

func (iv *writeOnlyInterval) openFreshSegment(s *Schema) error {
	defer func() { iv.NumSegments++ }()

	if !iv.DiskBacked {
		iv.CurSegment = new(bytes.Buffer)
		return nil
	}

	filename := iv.SegmentFilename(s, iv.NumSegments)
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	iv.CurSegment = f
	return nil
}

func (iv *writeOnlyInterval) closeCurrentSegment() error {
	if iv.DiskBacked {
		return iv.CurSegment.(*os.File).Close()
	}
	iv.buffers = append(iv.buffers, iv.CurSegment.(*bytes.Buffer))
	return nil
}

func (iv *Interval) SegmentFilename(s *Schema, segmentIndex int) string {
	name := fmt.Sprintf("interval.%d.generation%04d.segment%04d.dat",
		iv.Start.Unix(), iv.Generation, segmentIndex)
	return filepath.Join(s.Dir, name)
}

// WriteMemInterval writes out the data in memInterval to a fresh Interval with generation 0. Note that no
// interval with this start time should exist.
func (s *Schema) WriteMemInterval(memInterval *MemInterval) (*Interval, error) {
	cursor, err := memInterval.Tree.SeekFirst()
	if err != nil {
		return nil, err
	}
	interval := newWriteOnlyInterval(s.DiskBacked, 0, memInterval.Start, memInterval.End)
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

// WriteCombinedInterval writes out the combined data from memInterval and staticInterval to a fresh Interval
// with generation staticInterval.Generation+1.
func (s *Schema) WriteCombinedInterval(memInterval *MemInterval,
	staticInterval *Interval) (*Interval, error) {

	start := time.Now()
	// Sanity check
	if !memInterval.Start.Equal(staticInterval.Start) || !memInterval.End.Equal(staticInterval.End) {
		panic("attempt to combine memInterval/staticInterval from different times")
	}

	memCursor, err := memInterval.Tree.SeekFirst()
	if err != nil {
		return nil, err
	}
	staticCursor := staticInterval.cursor(s)
	interval := newWriteOnlyInterval(s.DiskBacked, staticInterval.Generation+1,
		memInterval.Start, memInterval.End)

	// Do an initial read from both mem and static, then loop and compare, advancing one or both (a classic
	// merge).
	moreMemKeys := true
	moreStaticKeys := true
	memKey, memVal, err := memCursor.Next()
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
		moreMemKeys = false
	}
	staticKey, staticVal, staticCount, more := staticCursor.Next()
	if !more {
		moreStaticKeys = false
	}

	var numMemRows, numStaticRows, numCombinedRows int

	for moreMemKeys && moreStaticKeys {
		cmp := bytes.Compare(memKey, staticKey)
		var advanceMem, advanceStatic bool
		switch {
		case cmp < 0:
			numMemRows++
			if err := interval.appendRow(s, memKey, memVal.Metric, memVal.Count); err != nil {
				return nil, err
			}
			advanceMem = true
		case cmp > 0:
			numStaticRows++
			if err := interval.appendRow(s, staticKey, staticVal, staticCount); err != nil {
				return nil, err
			}
			advanceStatic = true
		default: // equal
			numCombinedRows++
			metrics := make(MetricBytes, len(memVal.Metric))
			copy(metrics, memVal.Metric)
			metrics.add(s, MetricBytes(staticVal))
			count := memVal.Count + staticCount
			if err := interval.appendRow(s, memKey, metrics, count); err != nil {
				return nil, err
			}
			advanceMem = true
			advanceStatic = true
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
		if advanceStatic {
			staticKey, staticVal, staticCount, more = staticCursor.Next()
			if !more {
				moreStaticKeys = false
			}
		}
	}

	// Possibly drain leftover key/vals from memInterval or staticInterval.

	if moreMemKeys {
		for {
			numMemRows++
			if err = interval.appendRow(s, memKey, memVal.Metric, memVal.Count); err != nil {
				return nil, err
			}
			memKey, memVal, err = memCursor.Next()
			if err != nil {
				if err != io.EOF {
					return nil, err
				}
				break
			}
		}
	}
	if moreStaticKeys {
		for {
			numStaticRows++
			if err := interval.appendRow(s, staticKey, staticVal, staticCount); err != nil {
				return nil, err
			}
			staticKey, staticVal, staticCount, more = staticCursor.Next()
			if !more {
				break
			}
		}
	}

	totalSourceMemRows := numMemRows + numCombinedRows
	totalSourceStaticRows := numStaticRows + numCombinedRows
	totalResultRows := numMemRows + numStaticRows + numCombinedRows
	Log.Printf("Combined interval: %d mem rows and %d static rows written into %d total rows (%d rows combined)",
		totalSourceMemRows, totalSourceStaticRows, totalResultRows, numCombinedRows)
	Log.Printf("Combining interval took %s", time.Since(start))

	return interval.freeze(s)
}
