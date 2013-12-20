package main

import (
	"encoding/json"
	"fmt"
	"gumshoe/core"
	"io/ioutil"
	"net/http"
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

func handleTableRoute(responseWriter http.ResponseWriter, request *http.Request) {
	if request.Method != "GET" {
		http.Error(responseWriter, "", 404)
		return
	}
	results := make([]map[string]core.Untyped, 0, len(table.Rows))
	for _, row := range table.Rows {
		results = append(results, core.DenormalizeRow(table, &row))
	}
	writeJsonResponse(responseWriter, &results)
}

func handleQueryRoute(responseWriter http.ResponseWriter, request *http.Request) {
	if request.Method != "POST" {
		http.Error(responseWriter, "", 404)
		return
	}
	requestBody, _ := ioutil.ReadAll(request.Body)
	query, error := core.ParseJsonQuery(string(requestBody))
	if error != nil {
		fmt.Println(error)
		http.Error(responseWriter, error.Error(), 500)
	}
	results := core.InvokeQuery(table, query)
	writeJsonResponse(responseWriter, results)
}

func main() {
	fmt.Println(core.ROWS)
	// TODO(philc): Make these REST routes more thoughtful & consistent.
	http.HandleFunc("/insert", handleInsertRoute)
	http.HandleFunc("/tables/fact", handleTableRoute)
	http.HandleFunc("/tables/fact/query", handleQueryRoute)
	http.ListenAndServe(":9000", nil)
}
