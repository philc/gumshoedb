package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"gumshoe"
)

type Config struct {
	ListenAddr       string      `toml:"listen_addr"`
	DatabaseDir      string      `toml:"database_dir"`
	FlushDuration    Duration    `toml:"flush_duration"`
	TimestampColumn  [2]string   `toml:"timestamp_column"`
	DimensionColumns [][2]string `toml:"dimension_columns"`
	MetricColumns    [][2]string `toml:"metric_columns"`
}

// Produces a Schema based on the config file's values.
func (c *Config) ToSchema() (*gumshoe.Schema, error) {
	name, typ, isString := parseColumn(c.TimestampColumn)
	if typ != "uint32" {
		return nil, fmt.Errorf("timestamp column (%q) must be uint32", name)
	}
	if isString {
		return nil, errors.New("timestamp column cannot be a string")
	}
	timestampColumn, err := gumshoe.MakeDimensionColumn(name, typ, isString)
	if err != nil {
		return nil, err
	}

	dimensions := make([]gumshoe.DimensionColumn, len(c.DimensionColumns))
	for i, colPair := range c.DimensionColumns {
		name, typ, isString := parseColumn(colPair)
		if isString {
			switch typ {
			case "uint8", "uint16", "uint32":
			default:
				return nil, fmt.Errorf("got type %q for column %q (must be unsigned int type)", typ, name)
			}
		}
		col, err := gumshoe.MakeDimensionColumn(name, typ, isString)
		if err != nil {
			return nil, err
		}
		dimensions[i] = col
	}

	if len(c.MetricColumns) == 0 {
		return nil, fmt.Errorf("schema must include at least one metric column")
	}
	metrics := make([]gumshoe.MetricColumn, len(c.MetricColumns))
	for i, colPair := range c.MetricColumns {
		name, typ, isString := parseColumn(colPair)
		if isString {
			return nil, fmt.Errorf("metric column (%q) has string type; not allowed for metric columns", name)
		}
		col, err := gumshoe.MakeMetricColumn(name, typ)
		if err != nil {
			return nil, err
		}
		metrics[i] = col
	}

	// Check that we haven't duplicated any column names
	names := map[string]bool{timestampColumn.Name: true}
	for _, col := range dimensions {
		if names[col.Name] {
			return nil, fmt.Errorf("duplicate column name %q", col.Name)
		}
		names[col.Name] = true
	}
	for _, col := range metrics {
		if names[col.Name] {
			return nil, fmt.Errorf("duplicate column name %q", col.Name)
		}
		names[col.Name] = true
	}

	return &gumshoe.Schema{
		TimestampColumn:  timestampColumn.Column,
		DimensionColumns: dimensions,
		MetricColumns:    metrics,
		SegmentSize:      1e6,
		Dir:              c.DatabaseDir,
		FlushDuration:    c.FlushDuration.Duration,
	}, nil
}

func parseColumn(col [2]string) (name, typ string, isString bool) {
	name = col[0]
	typ = col[1]
	if strings.HasPrefix(typ, "string:") {
		typ = strings.TrimPrefix(typ, "string:")
		isString = true
	}
	return
}

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}
