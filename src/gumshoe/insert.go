package gumshoe

import (
	"bytes"
	"os"
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
