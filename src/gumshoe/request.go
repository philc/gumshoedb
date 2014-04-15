package gumshoe

import "time"

// DB request methods (all named Get*) are for retrieving DB information at a high level.

// A Request is used to read data.
type Request struct {
	Resp chan *Response
}

// A Response is the response to a Request. The current StaticTable is returned back to the requester, along
// with a channel to indicate when the user is done using it.
type Response struct {
	StaticTable *StaticTable
	done        chan struct{}
}

func (r *Response) Done() {
	r.done <- struct{}{}
}

func (db *DB) MakeRequest() *Response {
	respCh := make(chan *Response)
	db.requests <- &Request{respCh}
	return <-respCh
}

func (db *DB) GetQueryResult(query *Query) ([]RowMap, error) {
	resp := db.MakeRequest()
	defer resp.Done()
	return resp.StaticTable.InvokeQuery(query)
}

func (db *DB) GetDimensionTables() map[string][]string {
	resp := db.MakeRequest()
	defer resp.Done()

	results := make(map[string][]string)
	for i, col := range db.DimensionColumns {
		if col.String {
			results[col.Name] = resp.StaticTable.DimensionTables[i].Values
		}
	}
	return results
}

func (db *DB) GetLatestTimestamp() time.Time {
	db.latestTimestampLock.Lock()
	defer db.latestTimestampLock.Unlock()
	return db.latestTimestamp
}

func (db *DB) GetOldestIntervalTimestamp() time.Time {
	resp := db.MakeRequest()
	defer resp.Done()
	var oldest time.Time
	for t := range resp.StaticTable.Intervals {
		if oldest.IsZero() || t.Before(oldest) {
			oldest = t
		}
	}
	return oldest
}

func (db *DB) GetDebugStats() *StaticTableStats {
	resp := db.MakeRequest()
	defer resp.Done()
	return resp.StaticTable.stats()
}

func (db *DB) GetDebugPrint() {
	resp := db.MakeRequest()
	defer resp.Done()
	resp.StaticTable.debugPrint()
}

// GetDebugRows unpacks and returns up to the first 100 rows.
func (db *DB) GetDebugRows() []UnpackedRow {
	const max = 100

	resp := db.MakeRequest()
	defer resp.Done()

	var results []UnpackedRow
	for _, t := range resp.StaticTable.sortedIntervalTimes() {
		interval := resp.StaticTable.Intervals[t]
		for _, segment := range interval.Segments {
			for i := 0; i < len(segment.Bytes); i += db.RowSize {
				row := RowBytes(segment.Bytes[i : i+db.RowSize])
				unpacked := db.deserializeRow(row)
				// The RowMap doesn't have an attached timestamp column yet.
				unpacked.RowMap[db.TimestampColumn.Name] = uint32(t.Unix())
				results = append(results, unpacked)
				if len(results) == max {
					return results
				}
			}
		}
	}
	return results
}
