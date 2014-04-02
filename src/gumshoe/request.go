package gumshoe

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

func (db *DB) GetQueryResult(query *Query) ([]RowMap, error) {
	resp := db.makeRequest()
	defer resp.Done()

	return resp.State.InvokeQuery(query)
}
