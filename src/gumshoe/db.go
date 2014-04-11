package gumshoe

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

// OpenDB loads an existing DB. If schema.DiskBacked is false, this is the same as NewDB. Otherwise,
// schema.Dir must already contain a valid saved database. OpenDB sanity-checks the existing DB against the
// given schema.
func OpenDB(schema *Schema) (*DB, error) {
	if !schema.DiskBacked {
		return NewDB(schema)
	}

	metadataFilename := filepath.Join(schema.Dir, dbMetadataFilename)
	f, err := os.Open(metadataFilename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	db := new(DB)
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
	db.initialize()
	return db, nil
}

// NewDB creates a fresh DB. If it is disk-backed, the directory (schema.Dir) must not contain any existing DB
// files (*.json or *.dat).
func NewDB(schema *Schema) (*DB, error) {
	if !schema.DiskBacked {
		db := &DB{
			Schema: schema,
			State:  NewState(schema),
		}
		db.initialize()
		return db, nil
	}

	if err := os.Mkdir(schema.Dir, 0755); err != nil {
		if !os.IsExist(err) {
			return nil, err
		}
		for _, glob := range []string{"*.json", "*.dat"} {
			matches, err := filepath.Glob(filepath.Join(schema.Dir, glob))
			if err != nil {
				return nil, err
			}
			if len(matches) > 0 {
				return nil, fmt.Errorf("directory %s may already have a gumshoeDB database", schema.Dir)
			}
		}
	}
	db := &DB{
		Schema: schema,
		State:  NewState(schema),
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
}

// Flush triggers a DB flush and waits for it to complete.
func (db *DB) Flush() error {
	errCh := make(chan error)
	db.flushSignals <- errCh
	return <-errCh
}

// Close triggers a flush, waits for it to complete, and then shuts down the DB's goroutines. The DB may not
// be used again after calling Close.
func (db *DB) Close() error {
	if err := db.Flush(); err != nil {
		return err
	}
	close(db.shutdown)
	return nil
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
