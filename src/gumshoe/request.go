package gumshoe

// DB request methods (all named Get*) are for retrieving DB information at a high level.

// A Request is used to read data.
type Request struct {
	Resp chan *Response
}

// A Response is the response to a Request. The current state is returned back to the requester, along with a
// channel to indicate when the user is done using it.
type Response struct {
	State *State
	done  chan struct{}
}

func (r *Response) Done() {
	r.done <- struct{}{}
}

func (db *DB) makeRequest() *Response {
	respCh := make(chan *Response)
	db.requests <- &Request{respCh}
	return <-respCh
}

func (db *DB) GetQueryResult(query *Query) ([]RowMap, error) {
	resp := db.makeRequest()
	defer resp.Done()

	return resp.State.InvokeQuery(query)
}

func (db *DB) GetNumRows() int {
	resp := db.makeRequest()
	defer resp.Done()

	result := 0
	for _, interval := range resp.State.Intervals {
		result += interval.NumRows
	}
	return result
}

func (db *DB) GetCompressionRatio() float64 {
	resp := db.makeRequest()
	defer resp.Done()

	return resp.State.compressionRatio()
}

func (db *DB) GetDebugPrint() {
	resp := db.makeRequest()
	defer resp.Done()

	resp.State.debugPrint()
}

// GetDebugRows unpacks and returns up to the first 100 rows.
func (db *DB) GetDebugRows() []UnpackedRow {
	const max = 100

	resp := db.makeRequest()
	defer resp.Done()

	var results []UnpackedRow
	for _, t := range resp.State.sortedIntervalTimes() {
		interval := resp.State.Intervals[t]
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
