// A web server which runs the database as a daemon, and exposes routes for ingesting rows and executing
// queries.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"runtime"
	"time"
	"unsafe"

	"gumshoe"

	"github.com/BurntSushi/toml"
	"github.com/codegangsta/martini"
)

var (
	configFile = flag.String("config", "config.toml", "Configuration file to use")
	config     *Config
)

// This table is referenced by all of the routes.
var table *gumshoe.FactTable

func writeJsonResponse(responseWriter http.ResponseWriter, objectToSerialize interface{}) {
	jsonResult, err := json.Marshal(objectToSerialize)
	if err != nil {
		log.Print(err)
		http.Error(responseWriter, err.Error(), 500)
		return
	}
	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.Write(jsonResult)
}

// Given an array of JSON rows, insert them.
// TODO(philc): We need to enforce that there are no inserts happening concurrently. This http route should
// block until the previous insert is finished.
func handleInsertRoute(responseWriter http.ResponseWriter, request *http.Request) {
	if request.Method != "PUT" {
		http.Error(responseWriter, "", 404)
		return
	}

	decoder := json.NewDecoder(request.Body)
	jsonBody := []map[string]gumshoe.Untyped{}
	if err := decoder.Decode(&jsonBody); err != nil {
		log.Print(err)
		http.Error(responseWriter, err.Error(), 500)
		return
	}
	log.Printf("Inserting %d rows", len(jsonBody))

	if err := table.InsertRowMaps(jsonBody); err != nil {
		log.Print(err)
		http.Error(responseWriter, err.Error(), 500)
		return
	}
}

// A debugging route to list the contents of a table. Returns up to 1000 rows.
func handleFactTableRoute(responseWriter http.ResponseWriter, request *http.Request) {
	// For now, only return up to 1000 rows. We can't serialize the entire table unless we stream the response,
	// and for debugging, we only need a few rows to inspect that importing is working correctly.
	maxRowsToReturn := 1000
	rowCount := int(math.Min(float64(table.Count), float64(maxRowsToReturn)))
	results := make([]map[string]gumshoe.Untyped, 0, rowCount)
	rows := table.Rows()
	for i := 0; i < rowCount; i++ {
		row := rows[i]
		results = append(results, table.DenormalizeRow(&row))
	}
	writeJsonResponse(responseWriter, &results)
}

// Returns the contents of the all of the dimensions tables, for use when debugging.
func handleDimensionsTableRoute(responseWriter http.ResponseWriter, request *http.Request) {
	// Assembles the map: {dimensionTableName => [ [0 value0] [1 value1] ... ]}
	results := make(map[string][][2]gumshoe.Untyped)
	for _, dimensionTable := range table.DimensionTables[:table.ColumnCount] {
		rows := make([][2]gumshoe.Untyped, 0, table.ColumnCount)
		for i, value := range dimensionTable.Rows {
			row := [2]gumshoe.Untyped{i, value}
			rows = append(rows, row)
		}
		results[dimensionTable.Name] = rows
	}
	writeJsonResponse(responseWriter, &results)
}

// Evaluate a query and returns an aggregated result set.
// See the README for the query JSON structure and the structure of the reuslts.
func handleQueryRoute(responseWriter http.ResponseWriter, request *http.Request) {
	start := time.Now()
	requestBody, _ := ioutil.ReadAll(request.Body)
	query, err := gumshoe.ParseJsonQuery(string(requestBody))
	if err != nil {
		log.Print(err)
		http.Error(responseWriter, err.Error(), 500)
		return
	}
	if err = gumshoe.ValidateQuery(table, query); err != nil {
		log.Print(err)
		http.Error(responseWriter, err.Error(), 500)
		return
	}
	results := table.InvokeQuery(query)
	results["duration"] = (time.Since(start)).Nanoseconds() / (1000.0 * 1000.0)
	writeJsonResponse(responseWriter, results)
}

type Metricz struct {
	FactTableRows   int
	FactTableBytes  int
	DimensionTables map[string]map[string]int
}

func handleMetricz(responseWriter http.ResponseWriter) {
	dimensionTables := make(map[string]map[string]int)
	for _, dimensionTable := range table.DimensionTables {
		if dimensionTable != nil {
			dimensionTables[dimensionTable.Name] = map[string]int{
				"Rows":  len(dimensionTable.Rows),
				"Bytes": len(dimensionTable.Rows) * int(unsafe.Sizeof(*dimensionTable)),
			}
		}
	}

	metriczJson, err := json.Marshal(&Metricz{
		FactTableRows:   table.Count,
		FactTableBytes:  table.Count * int(unsafe.Sizeof(gumshoe.FactRow{})),
		DimensionTables: dimensionTables,
	})
	if err != nil {
		log.Print(err)
		http.Error(responseWriter, err.Error(), 500)
	}

	// TODO(dmac) Format this json more nicely
	fmt.Fprintln(responseWriter, string(metriczJson))
}

// Loads the fact table from disk if it exists, or creates a new one.
func loadFactTable() *gumshoe.FactTable {
	var table *gumshoe.FactTable
	if _, err := os.Stat(config.TableFilePath + ".json"); os.IsNotExist(err) {
		log.Printf("Table \"%s\" does not exist, creating... ", config.TableFilePath)
		table = gumshoe.NewFactTable(config.TableFilePath, config.ColumnNames)
		table.SaveToDisk()
		log.Print("done.")
	} else {
		log.Printf("Loading \"%s\"... ", config.TableFilePath)
		table = gumshoe.LoadFactTableFromDisk(config.TableFilePath)
		log.Printf("loaded %d rows.", table.Count)
	}
	return table
}

func runBackgroundSaves() {
	for {
		time.Sleep(config.SaveDuration.Duration)
		log.Println("Saving to disk...")
		table.SaveToDisk()
		log.Println("...done saving to disk")
	}
}

func main() {
	flag.Parse()
	// Set configuration defaults
	config = &Config{
		TableFilePath: "db/table",
		SaveDuration:  duration{10 * time.Second},
	}
	if _, err := toml.DecodeFile(*configFile, config); err != nil {
		log.Fatal(err)
	}
	if err := config.Validate(); err != nil {
		log.Fatal(err)
	}

	// Use all available cores for servicing requests in parallel.
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	table = loadFactTable()
	m := martini.Classic()
	// Use a specific set of middlewares instead of the defaults. Note that we've removed panic recovery.
	m.Handlers(martini.Logger(), martini.Static("public"))

	// TODO(philc): Make these REST routes more consistent.
	m.Get("/metricz", handleMetricz)
	m.Put("/insert", handleInsertRoute)
	m.Get("/tables/facts", handleFactTableRoute)
	m.Get("/tables/dimensions", handleDimensionsTableRoute)
	m.Post("/tables/facts/query", handleQueryRoute)

	go runBackgroundSaves()

	log.Fatal(http.ListenAndServe(":9000", m))
}
