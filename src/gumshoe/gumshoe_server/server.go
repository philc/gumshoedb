package main

import (
	"encoding/json"
	"fmt"
	"gumshoe/core"
	"io/ioutil"
	"net/http"
	"time"
)

var table = core.NewFactTable([]string{"at", "country", "bid", "impression", "click", "install"})

func writeJsonResponse(responseWriter http.ResponseWriter, objectToSerialize interface{}) {
	jsonResult, _ := json.Marshal(objectToSerialize)
	// TODO(philc): set a json header
	// responseWriter.Header()["Content-Type"] = "application/json"
	fmt.Fprint(responseWriter, string(jsonResult))
}

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
	}

	error = core.InsertRowMaps(table, jsonBody)
	if error != nil {
		fmt.Println(error)
		http.Error(responseWriter, error.Error(), 500)
	}
}

func handleFactTableRoute(responseWriter http.ResponseWriter, request *http.Request) {
	if request.Method != "GET" {
		http.Error(responseWriter, "", 404)
		return
	}
	results := make([]map[string]core.Untyped, 0, len(table.Rows))
	for i := 0; i < table.Count; i++ {
		row := table.Rows[i]
		results = append(results, core.DenormalizeRow(table, &row))
	}
	writeJsonResponse(responseWriter, &results)
}

// Returns the contents of the all of the dimensions tables, for debugging purposes.
func handleDimensionsTableRoute(responseWriter http.ResponseWriter, request *http.Request) {
	if request.Method != "GET" {
		http.Error(responseWriter, "", 404)
		return
	}
	// Assembles {dimensionTableName => [ [0 value0] [1 value1] ... ]
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

func handleQueryRoute(responseWriter http.ResponseWriter, request *http.Request) {
	start := time.Now()
	if request.Method != "POST" {
		http.Error(responseWriter, "", 404)
		return
	}
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
	results := core.InvokeQuery(table, query)
	results["duration"] = (time.Since(start)).Nanoseconds() / (1000.0 * 1000.0)
	writeJsonResponse(responseWriter, results)
}

func main() {
	fmt.Println(core.ROWS)
	// TODO(philc): Make these REST routes more thoughtful & consistent.
	http.HandleFunc("/insert", handleInsertRoute)
	http.HandleFunc("/tables/facts", handleFactTableRoute)
	http.HandleFunc("/tables/dimensions", handleDimensionsTableRoute)
	http.HandleFunc("/tables/facts/query", handleQueryRoute)
	http.ListenAndServe(":9000", nil)
}
