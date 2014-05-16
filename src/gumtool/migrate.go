// Possible future enhancements:
// * Change the timestamp column name
// * Change an arbitrary column name

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"
	"time"

	"config"
	"gumshoe"
)

func init() {
	commandsByName["migrate"] = command{
		description: "migrate a GumshoeDB database to a new database with a different schema",
		fn:          migrate,
	}
}

func migrate(args []string) {
	flags := flag.NewFlagSet("gumtool migrate", flag.ExitOnError)
	oldDBPath := flags.String("old-db-path", "", "Path of old DB directory")
	newConfigFilename := flags.String("new-db-config", "", "Filename of new DB config file")
	parallelism := flags.Int("parallelism", 4, "Parallelism for reading old DB")
	numOpenFiles := flags.Int("rlimit-nofile", 10000, "The value to set RLIMIT_NOFILE")
	flags.Parse(args)

	// Attempt to raise the open file limit; necessary for big migrations
	setRlimit(*numOpenFiles)

	oldDB, err := gumshoe.OpenDBDir(*oldDBPath)
	if err != nil {
		log.Fatal(err)
	}
	defer oldDB.Close()

	f, err := os.Open(*newConfigFilename)
	if err != nil {
		log.Fatal(err)
	}
	_, schema, err := config.LoadTOMLConfig(f)
	if err != nil {
		log.Fatal(err)
	}
	newDB, err := gumshoe.NewDB(schema)
	if err != nil {
		log.Fatal(err)
	}
	defer newDB.Close()

	if err := migrateDBs(newDB, oldDB, *parallelism); err != nil {
		log.Fatal(err)
	}
	fmt.Println("done")
}

type timestampSegment struct {
	*gumshoe.Segment
	at time.Time
}

func migrateDBs(newDB, oldDB *gumshoe.DB, parallelism int) error {
	resp := oldDB.MakeRequest()
	defer resp.Done()

	allSegments := findSegments(resp.StaticTable)
	progress := NewProgress("segments processed", len(allSegments))
	progress.Print()
	segments := make(chan *timestampSegment)
	shutdown := make(chan struct{})
	var workerErr error
	errc := make(chan error)
	var wg sync.WaitGroup
	wg.Add(parallelism)
	conversionFunc, err := makeConversionFunc(newDB, oldDB)
	if err != nil {
		return err
	}

	for i := 0; i < parallelism; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case segment := <-segments:
					if err := migrateSegment(newDB, oldDB, segment, conversionFunc); err != nil {
						select {
						case errc <- err:
						case <-shutdown:
						}
						return
					}
					progress.Add(1)
				case <-shutdown:
					return
				}
			}
		}()
	}

outer:
	for _, segment := range allSegments {
		select {
		case err := <-errc:
			workerErr = err
			break outer
		default:
			select {
			case segments <- segment:
			case err := <-errc:
				workerErr = err
				break outer
			}
		}
	}

	close(shutdown)
	wg.Wait()
	progress.Clear()
	return workerErr
}

func findSegments(staticTable *gumshoe.StaticTable) []*timestampSegment {
	var segments []*timestampSegment
	for t, interval := range staticTable.Intervals {
		for _, segment := range interval.Segments {
			segments = append(segments, &timestampSegment{segment, t})
		}
	}
	return segments
}

func migrateSegment(newDB, oldDB *gumshoe.DB, segment *timestampSegment,
	convert func(gumshoe.UnpackedRow)) error {

	at := uint32(segment.at.Unix())
	rows := make([]gumshoe.UnpackedRow, 0, len(segment.Bytes)/oldDB.RowSize)
	for i := 0; i < len(segment.Bytes); i += oldDB.RowSize {
		row := gumshoe.RowBytes(segment.Bytes[i : i+oldDB.RowSize])
		unpacked := oldDB.DeserializeRow(row)
		// Attach a timestamp
		unpacked.RowMap[oldDB.TimestampColumn.Name] = at
		convert(unpacked)
		rows = append(rows, unpacked)
	}
	if err := newDB.InsertUnpacked(rows); err != nil {
		return err
	}
	return newDB.Flush()
}

func makeConversionFunc(newDB, oldDB *gumshoe.DB) (func(gumshoe.UnpackedRow), error) {
	var (
		dimensionsToConvert []gumshoe.DimensionColumn
		dimensionsToDelete  []gumshoe.DimensionColumn
		metricsToConvert    []gumshoe.MetricColumn
		metricsToDelete     []gumshoe.MetricColumn
	)
	for _, oldDim := range oldDB.DimensionColumns {
		newDimIndex, ok := newDB.DimensionNameToIndex[oldDim.Name]
		if !ok {
			dimensionsToDelete = append(dimensionsToDelete, oldDim)
			continue
		}
		newDim := newDB.DimensionColumns[newDimIndex]
		if err := conversionAllowable(newDim.Type, oldDim.Type); err != nil {
			return nil, err
		}
		if newDim.String != oldDim.String {
			return nil, fmt.Errorf("conversion from string to non-string columns and vice versa are not allowed")
		}
		dimensionsToConvert = append(dimensionsToConvert, newDim)
	}
	if len(dimensionsToConvert) == 0 {
		return nil, fmt.Errorf("no common dimensions -- bad migration schemas?")
	}
	for _, oldMetric := range oldDB.MetricColumns {
		newMetricIndex, ok := newDB.MetricNameToIndex[oldMetric.Name]
		if !ok {
			metricsToDelete = append(metricsToDelete, oldMetric)
			continue
		}
		newMetric := newDB.MetricColumns[newMetricIndex]
		if err := conversionAllowable(newMetric.Type, oldMetric.Type); err != nil {
			return nil, err
		}
		metricsToConvert = append(metricsToConvert, newMetric)
	}

	fn := func(row gumshoe.UnpackedRow) {
		convertValueToFloat64(row.RowMap, newDB.TimestampColumn.Name)
		for _, dim := range dimensionsToDelete {
			delete(row.RowMap, dim.Name)
		}
		for _, dim := range dimensionsToConvert {
			if dim.String {
				continue
			}
			// Convert all numeric types to float64, as that is what the current serialization mechanism expects (as
			// typically inserted rows come from deserialized JSON).
			// TODO(caleb): It would be nice to avoid the intermediate float64 step when deserializing and
			// subsequently serializing rows as part of migration. I think this makes sense to do as part of a
			// general cleanup of row insertion/serialization where we separate the JSON (floaty) format from a more
			// strictly typed representation.
			value := row.RowMap[dim.Name]
			if value == nil {
				continue
			}
			convertValueToFloat64(row.RowMap, dim.Name)
		}

		for _, metric := range metricsToDelete {
			delete(row.RowMap, metric.Name)
		}
		for _, metric := range metricsToConvert {
			convertValueToFloat64(row.RowMap, metric.Name)
		}
	}
	return fn, nil
}

func conversionAllowable(to, from gumshoe.Type) error {
	if gumshoe.TypeToBigType[to] != gumshoe.TypeToBigType[from] {
		return fmt.Errorf("conversion from int to float types and vice versa are not allowed")
	}
	return nil
}

var float64Type = reflect.TypeOf(float64(0))

func convertValueToFloat64(rowMap gumshoe.RowMap, name string) {
	rv := reflect.ValueOf(rowMap[name])
	rowMap[name] = rv.Convert(float64Type).Interface()
}
