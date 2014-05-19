package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"config"
)

func TestSanity(t *testing.T) {
	const configText = `
listen_addr = ""
database_dir = "MEMORY"
flush_interval = "1h"
statsd_addr = "localhost:8125"
open_file_limit = 1000
query_parallelism = 10
retention_days = 7

[schema]
segment_size = "1MB"
interval_duration = "1h"
timestamp_column = ["at", "uint32"]
dimension_columns = [["dim1", "uint32"]]
metric_columns = [["metric1", "uint32"]]
	`
	conf, schema, err := config.LoadTOMLConfig(strings.NewReader(configText))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewServer(conf, schema))
	defer server.Close()

	resp, err := http.Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Error("Expected 200 at /")
	}
	resp.Body.Close()
}
