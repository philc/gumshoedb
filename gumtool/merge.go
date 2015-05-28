package main

import (
	"flag"
	"log"
	"os"

	"github.com/cespare/wait"

	"github.com/philc/gumshoedb/gumshoe"
	"github.com/philc/gumshoedb/internal/config"
)

func init() {
	commandsByName["merge"] = command{
		description: "merge the data from one or more GumshoeDB databases into a new database",
		fn:          merge,
	}
}

func merge(args []string) {
	flags := flag.NewFlagSet("gumtool merge", flag.ExitOnError)
	var (
		newConfigFilename string
		oldDBPaths        stringsFlag
		parallelism       int
		numOpenFiles      int
		flushSegments     int
	)
	flags.StringVar(&newConfigFilename, "new-db-config", "", "Filename of the new DB config")
	flags.Var(&oldDBPaths, "db-paths", "Paths to dirs of DBs to merge")
	flags.IntVar(&parallelism, "parallelism", 4, "Parallelism for merge workers")
	flags.IntVar(&numOpenFiles, "rlimit-nofile", 10000, "Value for RLIMIT_NOFILE")
	flags.IntVar(&flushSegments, "flush-segments", 500, "Flush after merging each N segments")
	flags.Parse(args)

	if len(oldDBPaths) == 0 {
		log.Fatalln("Need at least one entry in -db-paths; got 0")
	}

	setRlimit(numOpenFiles)

	f, err := os.Open(newConfigFilename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	_, schema, err := config.LoadTOMLConfig(f)
	if err != nil {
		log.Fatal(err)
	}
	newDB, err := gumshoe.NewDB(schema)
	if err != nil {
		log.Fatal(err)
	}
	defer newDB.Close()

	dbs := make([]*gumshoe.DB, len(oldDBPaths))
	for i, path := range oldDBPaths {
		db, err := gumshoe.OpenDBDir(path)
		if err != nil {
			log.Fatalf("Error opening DB at %s: %s", path, err)
		}
		if err := db.Schema.Equivalent(schema); err != nil {
			log.Fatalf("Schema of DB at %s didn't match config at %s: %s", path, newConfigFilename, err)
		}
		dbs[i] = db
	}

	for _, db := range dbs {
		log.Printf("Merging db %s", db.Schema.Dir)
		if err := mergeDB(newDB, db, parallelism, flushSegments); err != nil {
			log.Fatalln("Error merging:", err)
		}
		db.Close()
	}
}

func mergeDB(newDB, db *gumshoe.DB, parallelism, flushSegments int) error {
	resp := db.MakeRequest()
	defer resp.Done()

	allSegments := findSegments(resp.StaticTable)
	progress := NewProgress("segments processed", len(allSegments))
	progress.Print()
	segments := make(chan *timestampSegment)
	var wg wait.Group
	for i := 0; i < parallelism; i++ {
		wg.Go(func(quit <-chan struct{}) error {
			for {
				select {
				case <-quit:
					return nil
				case segment, ok := <-segments:
					if !ok {
						return nil
					}
					if err := mergeSegment(newDB, db, segment); err != nil {
						return err
					}
					progress.Add(1)
				}
			}
		})
	}

	wg.Go(func(quit <-chan struct{}) error {
		flushSegmentCount := 0
		for _, segment := range allSegments {
			select {
			case <-quit:
				return nil
			default:
				select {
				case <-quit:
					return nil
				case segments <- segment:
					flushSegmentCount++
					if flushSegmentCount == flushSegments {
						flushSegmentCount = 0
						if err := newDB.Flush(); err != nil {
							return err
						}
					}
				}
			}
		}
		close(segments)
		return nil
	})

	err := wg.Wait()
	if err != nil {
		return err
	}
	return newDB.Flush()
}

func mergeSegment(newDB, db *gumshoe.DB, segment *timestampSegment) error {
	// NOTE(caleb): Have to do more nasty float conversion in this function. See NOTE(caleb) in migrate.go.
	at := float64(segment.at.Unix())
	rows := make([]gumshoe.UnpackedRow, 0, len(segment.Bytes)/db.RowSize)
	for i := 0; i < len(segment.Bytes); i += db.RowSize {
		row := gumshoe.RowBytes(segment.Bytes[i : i+db.RowSize])
		unpacked := db.DeserializeRow(row)
		unpacked.RowMap[db.TimestampColumn.Name] = at
		for _, dim := range db.Schema.DimensionColumns {
			if dim.String {
				continue
			}
			value := unpacked.RowMap[dim.Name]
			if value == nil {
				continue
			}
			convertValueToFloat64(unpacked.RowMap, dim.Name)
		}
		for _, dim := range db.Schema.MetricColumns {
			convertValueToFloat64(unpacked.RowMap, dim.Name)
		}
		rows = append(rows, unpacked)
	}
	return newDB.InsertUnpacked(rows)
}
