package gumshoe

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"b"
)

// A RowMap is the unpacked form of a gumshoeDB row.
type RowMap map[string]Untyped

type insertionRow struct {
	Timestamp  time.Time
	Dimensions DimensionBytes
	Metrics    MetricBytes
}

func (db *DB) HandleInserts() {
	for {
		select {
		case <-db.shutdown:
			return
		case insert := <-db.inserts:
			var err error
			for _, row := range insert.Rows {
				if err = db.insertRow(row); err != nil {
					break
				}
			}
			insert.Err <- err
		case done := <-db.flushSignals:
			db.flush()
			if done != nil {
				done <- struct{}{}
			}
		}
	}
}

// insertRow puts row into the memtable, combining with other rows if possible. This should only be called by
// the insertion goroutine.
func (db *DB) insertRow(rowMap RowMap) error {
	row, err := db.serializeRowMap(rowMap)
	if err != nil {
		return err
	}
	timestamp := row.Timestamp.Truncate(intervalDuration)
	interval, ok := db.memTable.Intervals[timestamp]
	if !ok {
		interval = &MemInterval{
			Start: timestamp,
			End:   timestamp.Add(intervalDuration),
			// Make a B+tree with bytes.Compare (lexicographical) as the key comparison function.
			Tree:  b.TreeNew(bytes.Compare),
		}
		db.memTable.Intervals[timestamp] = interval
	}
	value, ok := interval.Tree.Get([]byte(row.Dimensions))
	if ok {
		// This key already exists in the tree. Add the metrics; bump the count.
		MetricBytes(value.Metric).add(db.Schema, row.Metrics)
		value.Count++
	} else {
		value = b.MetricWithCount{
			Count:  1,
			Metric: []byte(row.Metrics),
		}
	}
	interval.Tree.Set([]byte(row.Dimensions), value)
	return nil
}

type times []time.Time

func (t times) Len() int           { return len(t) }
func (t times) Less(i, j int) bool { return t[i].Before(t[j]) }
func (t times) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }

// flush saves the current memTable to disk by combining with overlapping state intervals to create a new
// state. This should only be called by the insertion goroutine.
func (db *DB) flush() {
	// Collect the interval keys in the state and in the memTable
	stateKeys := make([]time.Time, 0, len(db.State.Intervals))
	for t := range db.State.Intervals {
		stateKeys = append(stateKeys, t)
	}
	memKeys := make([]time.Time, 0, len(db.memTable.Intervals))
	for t := range db.memTable.Intervals {
		memKeys = append(memKeys, t)
	}
	sort.Sort(times(stateKeys))
	sort.Sort(times(memKeys))

	// Walk the keys together to produce the new state intervals
	intervals := make(map[time.Time]*Interval)
	var intervalsForCleanup []*Interval
	for len(stateKeys) > 0 && len(memKeys) > 0 {
		stateKey := stateKeys[0]
		memKey := memKeys[0]
		switch {
		case stateKey.Before(memKey):
			// We reuse this interval directly; the data hasn't changed.
			intervals[stateKey] = db.State.Intervals[stateKey]
			stateKeys = stateKeys[1:]
		case memKey.Before(stateKey):
			iv, err := db.WriteMemInterval(db.memTable.Intervals[memKey])
			if err != nil {
				// TODO(caleb): handle
				panic("uh-oh")
			}
			intervals[memKey] = iv
			memKeys = memKeys[1:]
		default: // equal
			stateInterval := db.State.Intervals[stateKey]
			memInterval := db.memTable.Intervals[memKey]
			iv, err := db.WriteCombinedInterval(memInterval, stateInterval)
			if err != nil {
				// TODO(caleb): handle
				panic("uh-oh")
			}
			intervals[stateKey] = iv
			stateKeys = stateKeys[1:]
			memKeys = memKeys[1:]
			intervalsForCleanup = append(intervalsForCleanup, stateInterval)
		}
	}
	for _, stateKey := range stateKeys {
		intervals[stateKey] = db.State.Intervals[stateKey]
	}
	for _, memKey := range memKeys {
		iv, err := db.WriteMemInterval(db.memTable.Intervals[memKey])
		if err != nil {
			// TODO(caleb): handle
			panic("uh-oh")
		}
		intervals[memKey] = iv
	}

	// Make the new State
	newState := NewState(db.Schema)
	newState.Intervals = intervals
	newState.DimensionTables = db.combineDimensionTables()

	// Figure out the row count
	for _, interval := range intervals {
		for _, segment := range interval.Segments {
			newState.Count += len(segment.Bytes) / db.RowSize
		}
	}

	// Create the FlushInfo and send it over to the request handling goroutine which will make the swap and then
	// return a chan to wait on all requests currently running on the old state.
	allRequestsFinishedChan := make(chan chan struct{})
	db.flushes <- &FlushInfo{NewState: newState, AllRequestsFinishedChan: allRequestsFinishedChan}
	allRequestsFinished := <-allRequestsFinishedChan

	// Wait for all requests on the old state to be done.
	<-allRequestsFinished

	// Write out the metadata
	if err := db.writeMetadataFile(); err != nil {
		// TODO(caleb): Handle
		panic(err)
	}

	// Clean up any now-unused intervals (only associated with previous state).
	db.cleanUpOldIntervals(intervalsForCleanup)

	// Replace the MemTable with a fresh, empty one.
	db.memTable = NewMemTable(db.Schema)
}

func (db *DB) combineDimensionTables() []*DimensionTable {
	dimTables := NewDimensionTablesForSchema(db.Schema)
	for i, col := range db.DimensionColumns {
		if !col.String {
			continue
		}
		for _, value := range db.State.DimensionTables[i].Values {
			_, _ = dimTables[i].GetAndMaybeSet(value)
		}
		for _, value := range db.memTable.DimensionTables[i].Values {
			_, _ = dimTables[i].GetAndMaybeSet(value)
		}
	}
	return dimTables
}

func (db *DB) writeMetadataFile() error {
	filename := filepath.Join(db.Dir, dbMetadataFilename)
	tmpFilename := filename + ".tmp"
	f, err := os.Create(tmpFilename)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(f)
	if err := encoder.Encode(db); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpFilename, filename)
}

func (db *DB) cleanUpOldIntervals(intervals []*Interval) {
	for _, interval := range intervals {
		// Unmap, close, and delete all the segment files
		for i, segment := range interval.Segments {
			if err := segment.Bytes.Unmap(); err != nil {
				log.Println("cleanup error unmapping segment file:", err)
			}
			if err := segment.File.Close(); err != nil {
				log.Println("cleanup error closing segment file:", err)
			}
			if err := os.Remove(db.SegmentFilename(interval.Start, interval.Generation, i)); err != nil {
				log.Println("cleanup error deleting segment file:", err)
			}
		}
	}
}
