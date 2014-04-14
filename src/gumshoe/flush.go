package gumshoe

import (
	"encoding/json"
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
	// Whenever we make a new StaticTable, we must give it a querySegmentJobs reference.
	newStaticTable.querySegmentJobs = db.querySegmentJobs
	newStaticTable.Intervals = intervals
	if newStaticTable.DimensionTables, err = db.combineDimensionTables(); err != nil {
		return err
	}

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
				return &intervalWriterResponse{memKey, iv, err}
			}
			memKeys = memKeys[1:]
		default: // equal
			numCombinedIntervals++
			staticInterval := db.StaticTable.Intervals[staticKey]
			memInterval := db.memTable.Intervals[memKey]
			intervalWriterRequests <- func() *intervalWriterResponse {
				iv, err := db.WriteCombinedInterval(memInterval, staticInterval)
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

// combineDimensionTables returns a combined set of dimension tables appropriate for the schema from the
// memtable and static table's dimension tables. Any dimensions in which the memtable has no new entries are
// reused from the static table. Changed dimensions have a new dimension table created with the combined
// values and incremented generation and are written to disk.
func (db *DB) combineDimensionTables() ([]*DimensionTable, error) {
	dimTables := make([]*DimensionTable, len(db.DimensionColumns))
	for i, col := range db.DimensionColumns {
		if !col.String {
			continue
		}
		memDimTable := db.memTable.DimensionTables[i]
		staticDimTable := db.StaticTable.DimensionTables[i]
		if len(memDimTable.Values) == 0 {
			// No changes. Just use the old dimension table.
			dimTables[i] = staticDimTable
			continue
		}
		generation := staticDimTable.Generation + 1
		newDimTable := newDimensionTable(generation, append(staticDimTable.Values, memDimTable.Values...))
		dimTables[i] = newDimTable

		if db.DiskBacked {
			if err := newDimTable.Store(db.Schema, i); err != nil {
				return nil, err
			}

			// Delete the old dimension table's file. If the old generation is 0, then there is no corresponding
			// file.
			if staticDimTable.Generation > 0 {
				if err := os.Remove(staticDimTable.Filename(db.Schema, i)); err != nil {
					Log.Println("cleanup error deleting dimension table file:", err)
				}
			}
		}
	}
	return dimTables, nil
}

// writeMetadataFile serializes db to JSON and atomically writes it to disk by using an intermediate tempfile
// and moving it into place.
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
