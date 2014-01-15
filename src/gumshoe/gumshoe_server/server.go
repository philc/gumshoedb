// A web server which exposes routes for ingesting rows and executing queries.
package main

import (
	"encoding/json"
	"fmt"
	martini "github.com/codegangsta/martini"
	"gumshoe/core"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"runtime"
	"time"
)

// TODO(philc): Add the ability to specify schema and persistence settings via a config file.
const tableFilePath = "db/table"

// How often to persist tables to disk.
const saveDurationInSecs = 10

// This table is referenced by all of the routes.
var table *core.FactTable

func writeJsonResponse(responseWriter http.ResponseWriter, objectToSerialize interface{}) {
	jsonResult, _ := json.Marshal(objectToSerialize)
	responseWriter.Header().Set("Content-Type", "application/json")
	fmt.Fprint(responseWriter, string(jsonResult))
}

// Given an array of JSON rows, insert them.
func handleInsertRoute(responseWriter http.ResponseWriter, request *http.Request) {
	if request.Method != "PUT" {
		http.Error(responseWriter, "", 404)
		return
	}

	requestBody, _ := ioutil.ReadAll(request.Body)
	jsonBody := make([](map[string]core.Untyped), 0)
	error := json.Unmarshal([]byte(requestBody), &jsonBody)
	if error != nil {
		fmt.Println(error)
		http.Error(responseWriter, error.Error(), 500)
		return
	}
	fmt.Printf("Inserting %d rows\n", len(jsonBody))

	error = table.InsertRowMaps(jsonBody)
	if error != nil {
		fmt.Println(error)
		http.Error(responseWriter, error.Error(), 500)
		return
	}
}

// Save the database to disk, blocking until it's written.
func handleSaveRoute(request *http.Request) () {
	table.SaveToDisk()
}

// A debugging route to list the contents of a table. Returns up to 1000 rows.
func handleFactTableRoute(responseWriter http.ResponseWriter, request *http.Request) {
	// For now, only return up to 1000 rows. We can't serialize the entire table unless we stream the response,
	// and for debugging, we only need a few rows to inspect that importing is working correctly.
	maxRowsToReturn := 1000
	rowCount := int(math.Min(float64(table.Count), float64(maxRowsToReturn)))
	results := make([]map[string]core.Untyped, 0, rowCount)
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
	results := make(map[string][][2]core.Untyped)
	for _, dimensionTable := range(table.DimensionTables[:table.ColumnCount]) {
		rows := make([][2]core.Untyped, 0, table.ColumnCount)
		for i, value := range dimensionTable.Rows {
			row := [2]core.Untyped{i, value}
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
	query, error := core.ParseJsonQuery(string(requestBody))
	if error != nil {
		fmt.Println(error)
		http.Error(responseWriter, error.Error(), 500)
		return
	}
	if error = core.ValidateQuery(table, query); error != nil {
		fmt.Println(error)
		http.Error(responseWriter, error.Error(), 500)
		return
	}
	results := table.InvokeQuery(query)
	results["duration"] = (time.Since(start)).Nanoseconds() / (1000.0 * 1000.0)
	writeJsonResponse(responseWriter, results)
}

// Loads the fact table from disk if it exists, or creates a new one.
func loadFactTable() *core.FactTable {
	var table *core.FactTable
  if _, err := os.Stat(tableFilePath + ".json"); os.IsNotExist(err) {
		fmt.Printf("Table \"%s\" does not exist, creating... ", tableFilePath)
		table = core.NewFactTable(tableFilePath, columnNames)
		table.SaveToDisk()
		fmt.Println("done.")
	} else {
		fmt.Printf("Loading \"%s\"... ", tableFilePath)
		table = core.LoadFactTableFromDisk(tableFilePath)
		fmt.Printf("loaded %d rows.\n", table.Count)
	}
	return table
}

func main() {
	// Use all available cores for servicing requests in parallel.
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	table = loadFactTable()
	m := martini.Classic()
	// Use a specific set of middlewares instead of the defaults. Note that we've removed panic recovery.
	m.Handlers(martini.Logger(), martini.Static("public"))

	// TODO(philc): Make these REST routes more consistent.
  m.Post("/save", handleSaveRoute)
  m.Put("/insert", handleInsertRoute)
	m.Get("/tables/facts", handleFactTableRoute)
	m.Get("/tables/dimensions", handleDimensionsTableRoute)
	m.Post("/tables/facts/query", handleQueryRoute)
	// TODO(philc): Complain if we can't bind to this port
	http.ListenAndServe(":9000", m)
}
