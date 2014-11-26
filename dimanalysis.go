// +build ignore

package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"text/template"

	"github.com/dustin/go-humanize"
)

type Dimension struct {
	Name string
	TOML string
}

type TestConfig struct {
	Dir        string
	Dimensions []Dimension
}

const (
	workingDir  = "/mnt/data/gumshoe"
	logFileName = "dimanalysis.log"
)

var (
	nameRegex  = regexp.MustCompile(`^\["([^"]+)"`)
	testDir    = filepath.Join(workingDir, "tests")
	oldDBDir   = filepath.Join(workingDir, "db")
	oldDBSize  uint64
	dimensions []Dimension
	logf       *os.File
)

func dirSize(dir string) (uint64, error) {
	output, err := exec.Command("du", "-sb", dir).Output()
	if err != nil {
		return 0, err
	}
	parts := strings.Fields(string(output))
	return strconv.ParseUint(parts[0], 10, 64)
}

func runTest(dims []Dimension, name string) error {
	dir := filepath.Join(testDir, name)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	configFilePath := filepath.Join(dir, "config.toml")
	configFile, err := os.Create(configFilePath)
	if err != nil {
		return err
	}
	if err := configTmpl.Execute(configFile, TestConfig{dir, dims}); err != nil {
		return err
	}
	configFile.Close()

	cmd := exec.Command("./gumtool", "migrate", "-parallelism", "8", "-flush-segments", "200",
		"-old-db-path", oldDBDir, "-new-db-config", configFilePath)
	cmd.Stdout = logf
	cmd.Stderr = logf
	if err := cmd.Run(); err != nil {
		return err
	}
	size, err := dirSize(dir)
	if err != nil {
		return err
	}
	fmt.Printf("%s %s %.2f%%\n", name, humanize.Bytes(size), 100.0*float64(size)/float64(oldDBSize))
	return nil
}

func main() {
	for _, s := range strings.Split(dimensionsText, "\n") {
		s = strings.TrimSpace(s)
		if s != "" {
			name := nameRegex.FindStringSubmatch(s)[1]
			dimensions = append(dimensions, Dimension{name, s})
		}
	}
	var err error
	oldDBSize, err = dirSize(oldDBDir)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("old DB size: %s\n", humanize.Bytes(oldDBSize))

	logf, err = os.Create(logFileName)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := logf.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	for _, dim1 := range dimensions {
		var dims []Dimension
		for _, dim2 := range dimensions {
			if dim2 != dim1 {
				dims = append(dims, dim2)
			}
		}
		if err := runTest(dims, dim1.Name); err != nil {
			log.Fatal(err)
		}
	}
}

var configTmpl = template.Must(template.New("config").Parse(configText))

const configText = `
# This is an open port in our security group.
listen_addr = ":8080"

statsd_addr = "localhost:8126"

open_file_limit = 20000

# Where on disk to keep the data.
database_dir = "{{.Dir}}"

# Flush to disk at least this frequently.
flush_interval = "30s"

query_parallelism = 4

# Delete data older than this.
retention_days = 100

[schema]

segment_size = "10MB"

# Data is partitioned by time intervals of this size.
interval_duration = "1h"

# Every row must have a timestamp column.
timestamp_column = ["at", "uint32"]

dimension_columns = [
{{range .Dimensions}}{{.TOML}}
{{end}}
]

metric_columns = [
  ["auction_price", "float32"],
  ["bid", "uint32"],
  ["bid_per_positive", "float32"],
  ["bid_price", "float32"],
  ["click", "uint16"],
  ["custom_app_event", "uint16"],
  ["impression", "uint16"],
  ["install", "uint16"],
  ["no_bid", "uint32"],
  ["predicted_imp_to_click_rate", "float32"],
  ["predicted_imp_to_install_rate", "float32"],
  ["predicted_imp_to_preferred_app_event_rate", "float32"],
  ["revenue", "float32"],
  ["internal_no_bid_normalized", "float32"],
  ["internal_bid", "uint32"],
  ["internal_bid_price", "float32"],
  ["reengaged_custom_app_event", "uint16"],
  ["first_direct_custom_app_event", "uint16"],
  ["first_reengaged_custom_app_event", "uint16"]
]
`

const dimensionsText = `
  ["ab_tests", "string:uint16"],
  ["ad_group_id", "uint32"],
  ["app_genre", "string:uint16"],
  ["app_name", "string:uint32"],
  ["app_store_id", "string:uint32"],
  ["bid_request_at", "uint32"],
  ["channel_id", "uint8"],
  ["city", "string:uint16"],
  ["click_to_install_model_tag", "string:uint8"],
  ["country", "string:uint8"],
  ["creative_id", "uint16"],
  ["creative_size", "string:uint16"],
  ["creative_type", "string:uint8"],
  ["custom_app_event_name", "string:uint16"],
  ["customer_id", "uint16"],
  ["dest_app_id", "uint16"],
  ["device_family", "string:uint8"],
  ["gender", "string:uint8"],
  ["imp_to_click_model_tag", "string:uint8"],
  ["install_to_preferred_app_event_model_tag", "string:uint8"],
  ["language", "string:uint16"],
  ["no_bid_reason_enum", "uint8"],
  ["os", "string:uint8"],
  ["region", "string:uint16"],
  ["revenue_type", "string:uint8"],
  ["video_supported", "uint8"],
  ["year_of_birth", "uint16"],
`
