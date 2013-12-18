package main

import (
	"fmt"
	"gumshoe/core"
	"net/http"
	"encoding/json"
	"io/ioutil"
)

// func errorResponse(statusCode int, message String, responseWriter http.ResponseWriter) {
// 	fmt.Fprintf(responseWriter

func parseJsonBody(request *http.Request) {
	requestBody, error := ioutil.ReadAll(request.Body)
	if error != nil {
		fmt.Println(error)
	}
	jsonBody := new(core.Untyped)
	error = json.Unmarshal([]byte(requestBody), jsonBody)
	if error != nil {
		fmt.Println(error)
	}
	fmt.Println(*jsonBody)
}


func handleImport(responseWriter http.ResponseWriter, request *http.Request) {
	if request.Method != "PUT" {
		http.Error(responseWriter, "", 404)
		return
	}

	parseJsonBody(request)
}

func main() {
	fmt.Println(core.ROWS)
	http.HandleFunc("/import", handleImport)
	http.ListenAndServe(":9000", nil)
}
