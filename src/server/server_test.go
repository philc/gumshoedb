package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSanity(t *testing.T) {
	config := &Config{TableFilePath: "", ColumnNames: []string{}}
	server := httptest.NewServer(NewServer(config))
	defer server.Close()
	resp, err := http.Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Error("Expected 404 at /")
	}
	resp.Body.Close()
}
