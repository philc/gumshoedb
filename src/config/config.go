package config

import (
	"errors"
	"strings"
	"time"

	"gumshoe"
)

type Config struct {
	Rows             int        `toml:"rows"`
	TimestampColumn  string     `toml:"timestamp_column"`
	ListenAddr       string     `toml:"listen_addr"`
	TableFilePath    string     `toml:"table_file_path"`
	SaveDuration     Duration   `toml:"save_duration"`
	DimensionColumns [][]string `toml:"dimension_columns"`
	MetricColumns    [][]string `toml:"metric_columns"`
}

func (c *Config) Validate() error {
	if c.Rows <= 0 {
		return errors.New("Must set the row count to be > 0.")
	}
	if c.TimestampColumn == "" {
		return errors.New("Must provide the name of a timestamp column.")
	}
	if len(c.MetricColumns) == 0 {
		return errors.New("Must provide at least one metric column in your configuration.")
	}
	return nil
}

type Duration struct {
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
	for _, columnPair := range c.DimensionColumns {
		columnType := columnPair[1]
		if strings.HasPrefix(columnType, "string:") {
			columnType = strings.Replace(columnType, "string:", "", 1)
			schema.StringColumns = append(schema.StringColumns, columnPair[1])
		}
		schema.DimensionColumns[columnPair[0]] = stringToSchemaType[columnType]
	}
	for _, columnPair := range c.MetricColumns {
		schema.MetricColumns[columnPair[0]] = stringToSchemaType[columnPair[1]]
	}
	return schema
}

func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}
