package gumshoe

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

const dbMetadataFilename = "db.json"

type DB struct {
	*Schema
	State    *State    // Owned by the request goroutine
	memTable *MemTable // Owned by the inserter goroutine

	shutdown chan struct{} // To tell goroutines to exit by closing

	// The inserter reads from these two chans
	inserts      chan *InsertRequest
	flushSignals chan chan error

	// The request goroutine reads from these two chans
	requests chan *Request
	flushes  chan *FlushInfo
}

// Open loads an existing DB or else makes a new one.
// - If schema.Dir is "", then the DB will be in-memory only.
// - If schema.Dir doesn't exist, then a fresh, disk-backed DB will be created.
// - Otherwise, Open loads an existing DB from schema.Dir after sanity checking against the given schema.
func Open(schema *Schema) (*DB, error) {
	if schema.Dir == "" {
		// Memory-only
		db := &DB{
			Schema: schema,
			State:  NewState(schema),
		}
		db.initialize()
		return db, nil
	}

	metadataFilename := filepath.Join(schema.Dir, dbMetadataFilename)
	var db *DB
	f, err := os.Open(metadataFilename)
	switch {
	case err == nil:
		// Load the DB from the metadata file.
		defer f.Close()
		db = new(DB)
		decoder := json.NewDecoder(f)
		if err := decoder.Decode(db); err != nil {
			return nil, err
		}
		if err := db.Schema.Equivalent(schema); err != nil {
			return nil, err
		}
		if err := db.State.initialize(schema); err != nil {
			return nil, err
		}
		db.Schema = schema
	case os.IsNotExist(err):
		if err := os.Mkdir(schema.Dir, 0755); err != nil {
			if !os.IsExist(err) {
				return nil, err
			}
		}
		db = &DB{
			Schema: schema,
			State:  NewState(schema),
		}
	default:
		return nil, err
	}

	db.initialize()
	return db, nil
}

func (db *DB) initialize() {
	db.Schema.initialize()
	db.memTable = NewMemTable(db.Schema)
	db.shutdown = make(chan struct{})
	db.inserts = make(chan *InsertRequest)
	db.flushSignals = make(chan chan error)
	db.requests = make(chan *Request)
	db.flushes = make(chan *FlushInfo)

	go db.HandleRequests()
	go db.HandleInserts()
	go func() {
		// Not using a ticker because I want a fixed break between flushes even if they take a long time.
		timer := time.NewTimer(db.Schema.FlushDuration)
		for {
			select {
			case <-db.shutdown:
				return
			case <-timer.C:
				db.flushSignals <- nil
				timer.Reset(db.Schema.FlushDuration)
			}
		}
	}()
}

// Flush triggers a DB flush and waits for it to complete.
func (db *DB) Flush() error {
	errCh := make(chan error)
	db.flushSignals <- errCh
	return <-errCh
}

// Close triggers a flush, waits for it to complete, and then shuts down the DB's goroutines. The DB may not
// be used again after calling Close.
func (db *DB) Close() {
	db.Flush()
	close(db.shutdown)
}

type InsertRequest struct {
	Rows []RowMap
	Err  chan error
}

type FlushInfo struct {
	NewState                *State
	AllRequestsFinishedChan chan chan struct{}
}

func when(pred bool, c chan *Request) chan *Request {
	if pred {
		return c
	}
	return nil
}

func (db *DB) HandleRequests() {
	db.State.spinUpRequestWorkers()
	var req *Request
	for {
		select {
		case <-db.shutdown:
			return
		// Use the nil channel trick (nil chans are excluded from selects) to avoid blocking on pushing a
		// request to the state's request chan.
		case req = <-when(req == nil, db.requests):
		case when(req != nil, db.State.requests) <- req:
			db.State.wg.Add(1)
			req = nil
		case flushInfo := <-db.flushes:
			// Spin up a goroutine that waits for all requests on the current state to finish and pass the chan back
			// to the inserter goroutine.
			requestsFinished := make(chan struct{})
			go func(state *State) {
				state.wg.Wait()
				requestsFinished <- struct{}{}
			}(db.State)
			// Swap out the old state for the new -- the inserter goroutine can garbage collect the old one once all
			// requests have been processed.
			db.State = flushInfo.NewState
			db.State.spinUpRequestWorkers()
			flushInfo.AllRequestsFinishedChan <- requestsFinished
		}
	}
}

// Insert adds some rows into the database. It returns (and stops) on the first error encountered. Note that
// the data is only in the memtable (not necessarily on disk) when Insert returns.
func (db *DB) Insert(rows []RowMap) error {
	errc := make(chan error)
	db.inserts <- &InsertRequest{Rows: rows, Err: errc}
	return <-errc
}
