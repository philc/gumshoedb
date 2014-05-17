package gumshoe

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const MetadataFilename = "db.json"

// Log is a global logger instance. The user may replace it with a custom logger (e.g., a log.Logger) before
// calling any gumshoedb functions.
var Log Logger = nopLogger{}

type DB struct {
	*Schema
	dirFile *os.File // An open file handle to be flocked while the DB is open (nil unless disk-backed)

	StaticTable *StaticTable // Owned by the request goroutine
	memTable    *MemTable    // Owned by the inserter goroutine

	shutdown chan struct{} // To tell goroutines to exit by closing

	// The inserter reads from these two chans
	inserts      chan *InsertRequest
	flushSignals chan chan error

	// The request goroutine reads from these two chans
	requests chan *Request
	flushes  chan *FlushInfo

	latestTimestampLock *sync.Mutex
	// Latest inserted row timestamp.
	latestTimestamp time.Time
}

// OpenDB loads an existing DB. If schema.DiskBacked is false, this is the same as NewDB. Otherwise,
// schema.Dir must already contain a valid saved database. OpenDB sanity-checks the existing DB against the
// given schema.
func OpenDB(schema *Schema) (*DB, error) {
	if !schema.DiskBacked {
		return NewDB(schema)
	}
	return openDBDir(schema.Dir, schema)
}

// OpenDBDir loads an existing DB, discovering the schema from the data there.
func OpenDBDir(dir string) (*DB, error) { return openDBDir(dir, nil) }

var DBDoesNotExistErr = errors.New("db dir does not exist")

// openDBDir opens an existing DB directory. If schema is non-nil, it is checked against the schema in dir.
func openDBDir(dir string, schema *Schema) (*DB, error) {
	f, err := os.Open(filepath.Join(dir, MetadataFilename))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, DBDoesNotExistErr
		}
		return nil, err
	}
	defer f.Close()
	db := new(DB)
	decoder := json.NewDecoder(f)
	if err := decoder.Decode(db); err != nil {
		return nil, err
	}
	if schema != nil {
		if err := db.Schema.Equivalent(schema); err != nil {
			return nil, err
		}
		// We need to use the given Schema because the on-disk one has a blank RunConfig.
		db.Schema = schema
	}
	db.Schema.DiskBacked = true
	db.Schema.Dir = dir
	if err := db.StaticTable.initialize(db.Schema); err != nil {
		return nil, err
	}
	if err := db.initialize(); err != nil {
		return nil, err
	}
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
		if err := db.initialize(); err != nil {
			return nil, err
		}
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
	if err := db.initialize(); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *DB) initialize() error {
	if db.DiskBacked {
		if err := db.addFlock(); err != nil {
			return err
		}
	}

	db.Schema.initialize()
	db.memTable = NewMemTable(db.Schema)
	db.shutdown = make(chan struct{})
	db.inserts = make(chan *InsertRequest)
	db.flushSignals = make(chan chan error)
	db.requests = make(chan *Request)
	db.flushes = make(chan *FlushInfo)
	db.latestTimestampLock = new(sync.Mutex)

	go db.HandleRequests()
	go db.HandleInserts()
	return nil
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
	if db.DiskBacked {
		return db.removeFlock()
	}
	return nil
}

func (db *DB) addFlock() error {
	f, err := os.Open(db.Dir)
	if err != nil {
		return err
	}
	db.dirFile = f
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("cannot lock database dir; is it currently in use? (err = %v)", err)
	}
	return nil
}

func (db *DB) removeFlock() error {
	defer db.dirFile.Close()
	return syscall.Flock(int(db.dirFile.Fd()), syscall.LOCK_UN)
}

type InsertRequest struct {
	Rows []UnpackedRow
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

// InsertUnpacked is like insert, but takes UnpackedRows (which have an associated count).
func (db *DB) InsertUnpacked(rows []UnpackedRow) error {
	errc := make(chan error)
	db.inserts <- &InsertRequest{Rows: rows, Err: errc}
	return <-errc
}

// Insert adds some rows into the database. It returns (and stops) on the first error encountered. Note that
// the data is only in the memtable (not necessarily on disk) when Insert returns.
func (db *DB) Insert(rows []RowMap) error {
	unpacked := make([]UnpackedRow, len(rows))
	for i, row := range rows {
		unpacked[i] = UnpackedRow{row, 1}
	}
	return db.InsertUnpacked(unpacked)
}
