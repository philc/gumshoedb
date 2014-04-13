package gumshoe

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"

	"b"
)

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
			insert.Err <- db.insertRows(insert.Rows)
		case errCh := <-db.flushSignals:
			errCh <- db.flush()
		}
	}
}

// insertRows puts each row into the memtable, combining with other rows if possible. This should only be
// called by the insertion goroutine.
func (db *DB) insertRows(rows []RowMap) error {
	Log.Printf("Inserting %d rows", len(rows))
	insertedRows := 0
	droppedOldRows := 0
	for _, rowMap := range rows {
		row, err := db.serializeRowMap(rowMap)
		if err != nil {
			return err
		}
		timestamp := row.Timestamp.Truncate(db.IntervalDuration)
		// Drop the row if it's out of retention
		if db.FixedRetention && db.intervalStartOutOfRetention(timestamp) {
			droppedOldRows++
			continue
		}

		interval, ok := db.memTable.Intervals[timestamp]
		if !ok {
			interval = &MemInterval{
				Start: timestamp,
				End:   timestamp.Add(db.IntervalDuration),
				// Make a B+tree with bytes.Compare (lexicographical) as the key comparison function.
				Tree: b.TreeNew(bytes.Compare),
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
		insertedRows++
	}
	Log.Printf("Inserted %d rows succesfully; dropped %d out-of-retention rows", insertedRows, droppedOldRows)
	return nil
}

type times []time.Time

func (t times) Len() int           { return len(t) }
func (t times) Less(i, j int) bool { return t[i].Before(t[j]) }
func (t times) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }

// flush saves the current memTable to disk by combining with overlapping static intervals to create a new
// StaticTable. This should only be called by the insertion goroutine.
//
// If db.FixedRetention is set, then flush will discard old intervals while constructing the new StaticTable.
//
// If the result error is not nil, the state of database may not be well-defined and a user should clean up
// any extraneous segment files not referenced by the metadata. (Note the new metadata is written at the end,
// atomically, so it should be used as the source of truth for which segments should be used and which
// discarded. 'gumtool clean' can perform this task.)
func (db *DB) flush() error {
	start := time.Now()
	defer func() {
		Log.Printf("Flush completed in %s", time.Since(start))
	}()

	// Collect the interval keys in the StaticTable and MemTable.
	staticKeys := make([]time.Time, 0, len(db.StaticTable.Intervals))
	for t := range db.StaticTable.Intervals {
		staticKeys = append(staticKeys, t)
	}
	memKeys := make([]time.Time, 0, len(db.memTable.Intervals))
	for t := range db.memTable.Intervals {
		memKeys = append(memKeys, t)
	}
	sort.Sort(times(staticKeys))
	sort.Sort(times(memKeys))

	var intervalsForCleanup []*Interval

	// If we're using a fixed retention, drop old intervals.
	if db.FixedRetention {
		var outdatedStaticKeys, outdatedMemKeys []time.Time
		outdatedStaticKeys, staticKeys = db.partitionIntervalStartsByRetention(staticKeys)
		outdatedMemKeys, memKeys = db.partitionIntervalStartsByRetention(memKeys)
		Log.Printf("Flush: ignoring %d mem intervals and %d static intervals out of retention",
			len(outdatedMemKeys), len(outdatedStaticKeys))

		for _, key := range outdatedStaticKeys {
			intervalsForCleanup = append(intervalsForCleanup, db.StaticTable.Intervals[key])
		}
	}
	Log.Printf("Flushing %d mem intervals into %d static intervals", len(memKeys), len(staticKeys))

	// Walk the keys together to produce the new static intervals
	intervals, cleanup, err := db.combineSortedMemStaticIntervals(memKeys, staticKeys)
	if err != nil {
		return err
	}
	intervalsForCleanup = append(intervalsForCleanup, cleanup...)
	Log.Printf("Flushing %d total intervals and cleaning up %d obsolete or out-of-retention intervals",
		len(intervals), len(intervalsForCleanup))

	// Make the new StaticTable
	newStaticTable := NewStaticTable(db.Schema)
	newStaticTable.Intervals = intervals
	newStaticTable.DimensionTables = db.combineDimensionTables()

	// Figure out the row count
	for _, interval := range intervals {
		for _, segment := range interval.Segments {
			newStaticTable.Count += len(segment.Bytes) / db.RowSize
		}
	}

	// Create the FlushInfo and send it over to the request handling goroutine which will make the swap and then
	// return a chan to wait on all requests currently running on the old StaticTable.
	allRequestsFinishedChan := make(chan chan struct{})
	db.flushes <- &FlushInfo{NewStaticTable: newStaticTable, AllRequestsFinishedChan: allRequestsFinishedChan}
	allRequestsFinished := <-allRequestsFinishedChan

	// Wait for all requests on the old StaticTable to be done.
	<-allRequestsFinished

	if db.DiskBacked {
		// Write out the metadata
		if err := db.writeMetadataFile(); err != nil {
			return err
		}

		// Clean up any now-unused intervals (only associated with previous StaticTable).
		db.cleanUpOldIntervals(intervalsForCleanup)
	}

	// Replace the MemTable with a fresh, empty one.
	db.memTable = NewMemTable(db.Schema)
	return nil
}

// combineSortedMemStaticIntervals combines all the intervals corresponding to memKeys and staticKeys. These
// must be in sorted order. This function returns the new interval set, a list of static intervals for
// cleanup, and any error that occurs.
func (db *DB) combineSortedMemStaticIntervals(memKeys, staticKeys []time.Time) (
	intervals map[time.Time]*Interval, intervalsForCleanup []*Interval, err error) {

	var numMemIntervals, numStaticIntervals, numCombinedIntervals int
	intervals = make(map[time.Time]*Interval)
	for len(staticKeys) > 0 && len(memKeys) > 0 {
		staticKey := staticKeys[0]
		memKey := memKeys[0]
		switch {
		case staticKey.Before(memKey):
			// We reuse this interval directly; the data hasn't changed.
			numStaticIntervals++
			intervals[staticKey] = db.StaticTable.Intervals[staticKey]
			staticKeys = staticKeys[1:]
		case memKey.Before(staticKey):
			numMemIntervals++
			iv, err := db.WriteMemInterval(db.memTable.Intervals[memKey])
			if err != nil {
				return nil, nil, err
			}
			intervals[memKey] = iv
			memKeys = memKeys[1:]
		default: // equal
			numCombinedIntervals++
			staticInterval := db.StaticTable.Intervals[staticKey]
			memInterval := db.memTable.Intervals[memKey]
			iv, err := db.WriteCombinedInterval(memInterval, staticInterval)
			if err != nil {
				return nil, nil, err
			}
			intervals[staticKey] = iv
			staticKeys = staticKeys[1:]
			memKeys = memKeys[1:]
			intervalsForCleanup = append(intervalsForCleanup, staticInterval)
		}
	}
	for _, staticKey := range staticKeys {
		numStaticIntervals++
		intervals[staticKey] = db.StaticTable.Intervals[staticKey]
	}
	for _, memKey := range memKeys {
		numMemIntervals++
		iv, err := db.WriteMemInterval(db.memTable.Intervals[memKey])
		if err != nil {
			return nil, nil, err
		}
		intervals[memKey] = iv
	}

	Log.Printf("Flush: using %d mem intervals and %d static intervals as-is and combining %d intervals",
		numMemIntervals, numStaticIntervals, numCombinedIntervals)

	return intervals, intervalsForCleanup, nil
}

func (db *DB) combineDimensionTables() []*DimensionTable {
	dimTables := NewDimensionTablesForSchema(db.Schema)
	for i, col := range db.DimensionColumns {
		if !col.String {
			continue
		}
		for _, value := range db.StaticTable.DimensionTables[i].Values {
			_, _ = dimTables[i].GetAndMaybeSet(value)
		}
		for _, value := range db.memTable.DimensionTables[i].Values {
			_, _ = dimTables[i].GetAndMaybeSet(value)
		}
	}
	return dimTables
}

func (db *DB) writeMetadataFile() error {
	filename := filepath.Join(db.Dir, MetadataFilename)
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
				Log.Println("cleanup error unmapping segment file:", err)
			}
			if err := segment.File.Close(); err != nil {
				Log.Println("cleanup error closing segment file:", err)
			}
			if err := os.Remove(interval.SegmentFilename(db.Schema, i)); err != nil {
				Log.Println("cleanup error deleting segment file:", err)
			}
		}
	}
}

func (db *DB) intervalStartOutOfRetention(timestamp time.Time) bool {
	return time.Since(timestamp.Add(db.IntervalDuration)) > db.Retention
}

func (db *DB) partitionIntervalStartsByRetention(keys []time.Time) (outdated, current []time.Time) {
	for _, key := range keys {
		if db.intervalStartOutOfRetention(key) {
			outdated = append(outdated, key)
		} else {
			current = append(current, key)
		}
	}
	return outdated, current
}
