package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"unsafe"

	"github.com/philc/gumshoedb/gumshoe"
)

func init() {
	commandsByName["stats"] = command{
		description: "compute some stats about a GumshoeDB database",
		fn:          stats,
	}
}

func stats(args []string) {
	flags := flag.NewFlagSet("gumtool stats", flag.ExitOnError)
	dir := flags.String("dir", "", "DB dir")
	parallelism := flags.Int("parallelism", 4, "Parallelism for reading the DB")
	numOpenFiles := flags.Int("rlimit-nofile", 10000, "The value to set RLIMIT_NOFILE")
	flags.Parse(args)

	if *dir == "" {
		fatalln("-dir must be provided")
	}

	setRlimit(*numOpenFiles)

	db, err := gumshoe.OpenDBDir(*dir)
	if err != nil {
		log.Fatal(err)
	}

	mins, maxes := getColumnExtrema(db, *parallelism)

	fmt.Println("Dimension columns:")
	printStatsRow("name", "min", "max")
	printStatsRow("----", "---", "---")
	for i, col := range db.DimensionColumns {
		printStatsRow(col.Name, mins[i], maxes[i])
	}

	fmt.Println("\nMetric columns:")
	printStatsRow("name", "min", "max")
	printStatsRow("----", "---", "---")
	offset := len(db.DimensionColumns)
	for i, col := range db.MetricColumns {
		printStatsRow(col.Name, mins[i+offset], maxes[i+offset])
	}
}

func printStatsRow(col1, col2, col3 interface{}) {
	fmt.Printf("%50v%15v%15v\n", col1, col2, col3)
}

func getColumnExtrema(db *gumshoe.DB, parallelism int) (mins, maxes []gumshoe.Untyped) {
	resp := db.MakeRequest()
	defer resp.Done()

	numColumns := len(db.DimensionColumns) + len(db.MetricColumns)
	allSegments := findSegments(resp.StaticTable)
	progress := NewProgress("segments processed", len(allSegments))
	progress.Print()
	segments := make(chan *timestampSegment)
	partialResults := make([]minsMaxes, parallelism)

	var wg sync.WaitGroup
	wg.Add(parallelism)
	for i := 0; i < parallelism; i++ {
		i := i
		go func() {
			defer wg.Done()

			partial := minsMaxes{
				Mins:  make([]gumshoe.Untyped, numColumns),
				Maxes: make([]gumshoe.Untyped, numColumns),
			}

			for segment := range segments {
				for j := 0; j < len(segment.Bytes); j += db.RowSize {
					dimensions := gumshoe.DimensionBytes(segment.Bytes[j+db.DimensionStartOffset : j+db.MetricStartOffset])
					for k, col := range db.DimensionColumns {
						if dimensions.IsNil(k) {
							continue
						}
						value := gumshoe.NumericCellValue(unsafe.Pointer(&dimensions[db.DimensionOffsets[k]]), col.Type)
						partial.update(value, k)
					}
					metrics := gumshoe.MetricBytes(segment.Bytes[j+db.MetricStartOffset : j+db.RowSize])
					for k, col := range db.MetricColumns {
						value := gumshoe.NumericCellValue(unsafe.Pointer(&metrics[db.MetricOffsets[k]]), col.Type)
						partial.update(value, k+len(db.DimensionColumns))
					}
				}
				progress.Add(1)
			}
			partialResults[i] = partial
		}()
	}

	for _, segment := range allSegments {
		segments <- segment
	}
	close(segments)
	wg.Wait()
	progress.Clear()

	return combinePartialStats(partialResults)
}

func combinePartialStats(partialResults []minsMaxes) (mins, maxes []gumshoe.Untyped) {
	total := partialResults[0]
	for i := 1; i < len(partialResults); i++ {
		for j, min := range total.Mins {
			total.update(min, j)
		}
		for j, max := range total.Maxes {
			total.update(max, j)
		}
	}
	return total.Mins, total.Maxes
}

type minsMaxes struct {
	Mins  []gumshoe.Untyped
	Maxes []gumshoe.Untyped
}

func (m *minsMaxes) update(value gumshoe.Untyped, index int) {
	if value == nil {
		return
	}
	min, max := m.Mins[index], m.Maxes[index]
	if min == nil || gumshoe.UntypedLess(value, min) {
		m.Mins[index] = value
	}
	if max == nil || gumshoe.UntypedLess(max, value) {
		m.Maxes[index] = value
	}
}
