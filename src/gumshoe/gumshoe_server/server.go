package main

import (
	"encoding/json"
	"fmt"
	"gumshoe/core"
	"io/ioutil"
	"net/http"
)

var table = core.NewFactTable([]string{"at", "country", "impression", "clicks"})

func handleImportRoute(responseWriter http.ResponseWriter, request *http.Request) {
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

// TODO(philc): Make this return JSON maps of rows, rather than denormalized vectors.
func handleTableRoute(responseWriter http.ResponseWriter, request *http.Request) {
	if request.Method != "GET" {
		http.Error(responseWriter, "", 404)
		return
	}
	jsonResult, _ := json.Marshal(table.Rows)
	fmt.Fprint(responseWriter, string(jsonResult))
}

func main() {
	fmt.Println(core.ROWS)
	http.HandleFunc("/import", handleImportRoute)
	http.HandleFunc("/tables", handleTableRoute)
	http.ListenAndServe(":9000", nil)
}
