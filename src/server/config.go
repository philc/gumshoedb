package main

import (
	"errors"
	"gumshoe"
	"time"
)

type Config struct {
	ListenAddr     string     `toml:"listen_addr"`
	TableFilePath  string     `toml:"table_file_path"`
	SaveDuration   duration   `toml:"save_duration"`
	NumericColumns [][]string `toml:"numeric_columns"`
	StringColumns  [][]string `toml:"string_columns"`
}

func (c *Config) Validate() error {
	if len(c.NumericColumns) == 0 && len(c.StringColumns) == 0 {
		return errors.New("Must provide at least one column in your configuration.")
	}
	return nil
}

type duration struct {
	time.Duration
}

var stringToSchemaType = map[string]int{
	"uint8":   gumshoe.Uint8Type,
	"int8":    gumshoe.Int8Type,
	"uint16":  gumshoe.Uint16Type,
	"int16":   gumshoe.Int16Type,
	"uint32":  gumshoe.Uint32Type,
	"int32":   gumshoe.Int32Type,
	"uint64":  gumshoe.Uint64Type,
	"int64":   gumshoe.Int64Type,
	"float32": gumshoe.Float32Type,
	"float64": gumshoe.Float64Type,
}

// Produces a Schema based on the config file's values.
func (c *Config) ToSchema() gumshoe.Schema {
	schema := *gumshoe.NewSchema()
	for _, columnPair := range c.NumericColumns {
		schema.NumericColumns[columnPair[0]] = stringToSchemaType[columnPair[1]]
	}
	return schema
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}
