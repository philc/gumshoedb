package gumshoe

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const MetadataFilename = "db.json"

// Log is a global logger instance. The user may replace it with a custom logger (e.g., a log.Logger) before
// calling any gumshoedb functions.
var Log Logger = nopLogger{}

type DB struct {
	*Schema
	StaticTable *StaticTable // Owned by the request goroutine
	memTable    *MemTable    // Owned by the inserter goroutine

	shutdown chan struct{} // To tell goroutines to exit by closing

	// The inserter reads from these two chans
	inserts      chan *InsertRequest
	flushSignals chan chan error

	// The request goroutine reads from these two chans
	requests chan *Request
	flushes  chan *FlushInfo

	// Queries scan segments in parallel on a fixed goroutine pool pulling from querySegmentJobs
	querySegmentJobs chan func()
}

// OpenDB loads an existing DB. If schema.DiskBacked is false, this is the same as NewDB. Otherwise,
// schema.Dir must already contain a valid saved database. OpenDB sanity-checks the existing DB against the
// given schema.
func OpenDB(schema *Schema) (*DB, error) {
	if !schema.DiskBacked {
		return NewDB(schema)
	}

	f, err := os.Open(filepath.Join(schema.Dir, MetadataFilename))
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
	if err := db.StaticTable.initialize(schema); err != nil {
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
			Schema:      schema,
			StaticTable: NewStaticTable(schema),
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
		Schema:      schema,
		StaticTable: NewStaticTable(schema),
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
	// querySegmentJobs is a big buffered channel because we want to let queries enqueue all their segments
	// segment funcs together rather than having them unpredictably distributed as many queries contend on a
	// single unbuffered channel. This way queries should be able to roughly maintain their FIFO priority.
	db.querySegmentJobs = make(chan func(), 1e5)
	// The current StaticTable always needs a to have querySegmentJobs. We set that reference here, and wherever
	// we create a new StaticTable (when flushing) we set it on that new one.
	db.StaticTable.querySegmentJobs = db.querySegmentJobs

	go db.HandleRequests()
	go db.HandleInserts()

	for i := 0; i < db.QueryParallelism; i++ {
		go db.runQuerySegmentWorker()
	}
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
	NewStaticTable          *StaticTable
	AllRequestsFinishedChan chan chan struct{}
}

func when(pred bool, c chan *Request) chan *Request {
	if pred {
		return c
	}
	return nil
}

func (db *DB) HandleRequests() {
	db.StaticTable.startRequestWorkers()
	var req *Request
	for {
		select {
		case <-db.shutdown:
			db.StaticTable.stopRequestWorkers()
			return
		// Use the nil channel trick (nil chans are excluded from selects) to avoid blocking on pushing a
		// request to the StaticTable's request chan.
		case req = <-when(req == nil, db.requests):
			db.StaticTable.wg.Add(1)
		case when(req != nil, db.StaticTable.requests) <- req:
			req = nil
		case flushInfo := <-db.flushes:
			if req != nil {
				// We'll grandfather req to the next StaticTable. Mark it as done for now and increment wg once the
				// new StaticTable is in place.
				db.StaticTable.wg.Done()
			}
			db.StaticTable.stopRequestWorkers()
			// Spin up a goroutine that waits for all requests on the current StaticTable to finish and pass the
			// chan back to the inserter goroutine.
			requestsFinished := make(chan struct{})
			go func(st *StaticTable) {
				st.wg.Wait()
				requestsFinished <- struct{}{}
			}(db.StaticTable)
			// Swap out the old StaticTable for the new -- the inserter goroutine can garbage collect the old one
			// once all requests have been processed.
			db.StaticTable = flushInfo.NewStaticTable
			db.StaticTable.startRequestWorkers()
			if req != nil {
				db.StaticTable.wg.Add(1)
			}
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
