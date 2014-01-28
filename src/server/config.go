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
	"uint8":   gumshoe.TypeUint8,
	"int8":    gumshoe.TypeInt8,
	"uint16":  gumshoe.TypeUint16,
	"int16":   gumshoe.TypeInt16,
	"uint32":  gumshoe.TypeUint32,
	"int32":   gumshoe.TypeInt32,
	"float32": gumshoe.TypeFloat32,
}

// Produces a Schema based on the config file's values.
func (c *Config) ToSchema() *gumshoe.Schema {
	schema := gumshoe.NewSchema()
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
