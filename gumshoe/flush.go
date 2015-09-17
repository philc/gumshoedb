package gumshoe

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

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

	// Walk the keys together to produce the new static intervals.
	intervals, cleanup, err := db.combineSortedMemStaticIntervals(memKeys, staticKeys)
	if err != nil {
		return fmt.Errorf("error combining mem+static intervals: %s", err)
	}
	intervalsForCleanup = append(intervalsForCleanup, cleanup...)
	Log.Printf("Flushing %d total intervals and cleaning up %d obsolete or out-of-retention intervals",
		len(intervals), len(intervalsForCleanup))

	// Combine and write out new generations of the dimension tables.
	newDimTables, oldDimTables, err := db.combineDimensionTables()
	if err != nil {
		return fmt.Errorf("cannot combine dimension tables: %s", err)
	}

	// Make the new StaticTable.
	newStaticTable := NewStaticTable(db.Schema)
	newStaticTable.Intervals = intervals
	newStaticTable.DimensionTables = newDimTables

	// Create the FlushInfo and send it over to the request handling goroutine which will make the swap and then
	// return a chan to wait on all requests currently running on the old StaticTable.
	allRequestsFinishedChan := make(chan chan struct{})
	db.flushes <- &FlushInfo{NewStaticTable: newStaticTable, AllRequestsFinishedChan: allRequestsFinishedChan}
	allRequestsFinished := <-allRequestsFinishedChan

	// Wait for all requests on the old StaticTable to be done.
	<-allRequestsFinished

	if db.DiskBacked {
		// Write out the metadata.
		if err := db.writeMetadataFile(); err != nil {
			return fmt.Errorf("error writing metadata: %s", err)
		}

		// Clean up any now-unused intervals and dimension tables (only associated with previous StaticTable).
		for _, dimTable := range oldDimTables {
			if err := os.Remove(dimTable.Filename(db.Schema, dimTable.i)); err != nil {
				Log.Println("cleanup error deleting dimension table file:", err)
			}
		}
		db.cleanUpOldIntervals(intervalsForCleanup)
	}

	// Replace the MemTable with a fresh, empty one.
	db.memTable = NewMemTable(db.Schema)
	return nil
}

const intervalWriterParallelism = 8

type intervalWriterResponse struct {
	key      time.Time
	interval *Interval
	err      error
}

// combineSortedMemStaticIntervals combines all the intervals corresponding to memKeys and staticKeys. These
// must be in sorted order. This function returns the new interval set, a list of static intervals for
// cleanup, and any error that occurs.
func (db *DB) combineSortedMemStaticIntervals(memKeys, staticKeys []time.Time) (
	intervals map[time.Time]*Interval, intervalsForCleanup []*Interval, err error) {

	// Spin up several goroutines to write out new intervals concurrently.
	intervalWriterRequests := make(chan func() *intervalWriterResponse)
	intervalWriterResponses := make(chan *intervalWriterResponse)
	wg := new(sync.WaitGroup)
	for i := 0; i < intervalWriterParallelism; i++ {
		wg.Add(1)
		go func() {
			for fn := range intervalWriterRequests {
				intervalWriterResponses <- fn()
			}
			wg.Done()
		}()
	}

	// intervals is only modified in the response receiver goroutine below.
	intervals = make(map[time.Time]*Interval)
	done := make(chan error)

	go func() {
		var err error
		for response := range intervalWriterResponses {
			// If we previously saw an error; still drain the response queue (but we'll report the first one).
			if err != nil {
				continue
			}
			if response.err != nil {
				err = response.err
				continue
			}
			intervals[response.key] = response.interval
		}
		done <- err
	}()

	var numMemIntervals, numStaticIntervals, numCombinedIntervals int
	for len(staticKeys) > 0 && len(memKeys) > 0 {
		staticKey := staticKeys[0]
		memKey := memKeys[0]
		switch {
		case staticKey.Before(memKey):
			// We reuse this interval directly; the data hasn't changed.
			numStaticIntervals++
			intervalWriterRequests <- func() *intervalWriterResponse {
				return &intervalWriterResponse{staticKey, db.StaticTable.Intervals[staticKey], nil}
			}
			staticKeys = staticKeys[1:]
		case memKey.Before(staticKey):
			numMemIntervals++
			intervalWriterRequests <- func() *intervalWriterResponse {
				iv, err := db.WriteMemInterval(db.memTable.Intervals[memKey])
				if err != nil {
					err = fmt.Errorf("cannot write mem interval: %s", err)
				}
				return &intervalWriterResponse{memKey, iv, err}
			}
			memKeys = memKeys[1:]
		default: // equal
			numCombinedIntervals++
			staticInterval := db.StaticTable.Intervals[staticKey]
			memInterval := db.memTable.Intervals[memKey]
			intervalWriterRequests <- func() *intervalWriterResponse {
				iv, err := db.WriteCombinedInterval(memInterval, staticInterval)
				if err != nil {
					err = fmt.Errorf("cannot write combined interval: %s", err)
				}
				return &intervalWriterResponse{staticKey, iv, err}
			}
			staticKeys = staticKeys[1:]
			memKeys = memKeys[1:]
			intervalsForCleanup = append(intervalsForCleanup, staticInterval)
		}
	}
	for _, staticKey := range staticKeys {
		numStaticIntervals++
		key := staticKey
		intervalWriterRequests <- func() *intervalWriterResponse {
			return &intervalWriterResponse{key, db.StaticTable.Intervals[key], nil}
		}
	}
	for _, memKey := range memKeys {
		numMemIntervals++
		key := memKey
		intervalWriterRequests <- func() *intervalWriterResponse {
			iv, err := db.WriteMemInterval(db.memTable.Intervals[key])
			if err != nil {
				err = fmt.Errorf("cannot write mem interval: %s", err)
			}
			return &intervalWriterResponse{key, iv, err}
		}
	}

	close(intervalWriterRequests)
	wg.Wait()
	close(intervalWriterResponses)
	if err := <-done; err != nil {
		return nil, nil, err
	}

	Log.Printf("Flush: using %d mem intervals and %d static intervals as-is and combining %d intervals",
		numMemIntervals, numStaticIntervals, numCombinedIntervals)

	return intervals, intervalsForCleanup, nil
}

// combineDimensionTables returns a combined set of dimension tables
// appropriate for the schema from the memtable and static table's dimension tables.
// Any dimensions in which the memtable has no new entries are reused from the static table.
// Changed dimensions have a new dimension table created with the combined values
// and incremented generation. The new dimension tables are written to disk.
//
// This function also returns oldTables, which are all the old static dimension tables
// which should be deleted from disk.
func (db *DB) combineDimensionTables() (newTables []*DimensionTable, oldTables []indexedDimensionTable, err error) {
	newTables = make([]*DimensionTable, len(db.DimensionColumns))
	for i, col := range db.DimensionColumns {
		if !col.String {
			continue
		}
		memTable := db.memTable.DimensionTables[i]
		staticTable := db.StaticTable.DimensionTables[i]
		if len(memTable.Values) == 0 {
			// No changes. Just use the old dimension table.
			newTables[i] = staticTable
			continue
		}
		generation := staticTable.Generation + 1
		newTable := newDimensionTable(generation, append(staticTable.Values, memTable.Values...))
		newTables[i] = newTable
		oldTables = append(oldTables, indexedDimensionTable{staticTable, i})
		if db.DiskBacked {
			if err := newTable.Store(db.Schema, i); err != nil {
				return nil, nil, fmt.Errorf("error storing dimension table: %s", err)
			}
		}
	}
	return newTables, oldTables, nil
}

type indexedDimensionTable struct {
	*DimensionTable
	i int
}

// writeMetadataFile serializes db to JSON and atomically writes it to disk by using an intermediate tempfile
// and moving it into place.
func (db *DB) writeMetadataFile() error {
	b, err := json.MarshalIndent(db, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshalling metadata JSON: %s", err)
	}
	filename := filepath.Join(db.Dir, MetadataFilename)
	tmpFilename := filename + ".tmp"
	if err := ioutil.WriteFile(tmpFilename, b, 0666); err != nil {
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

type times []time.Time

func (t times) Len() int           { return len(t) }
func (t times) Less(i, j int) bool { return t[i].Before(t[j]) }
func (t times) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
