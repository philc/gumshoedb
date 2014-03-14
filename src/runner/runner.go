// TODO(caleb): Delete

package main

import (
	"fmt"
	"log"
	"strings"

	"config"
	"gumshoe"

	"github.com/BurntSushi/toml"
)

func main() {
	config := &config.Config{}
	if _, err := toml.DecodeFile("runner_config.toml", config); err != nil {
		log.Fatal(err)
	}
	schema, err := config.ToSchema()
	if err != nil {
		log.Fatal(err)
	}
	db, err := gumshoe.Open(schema)
	if err != nil {
		log.Fatal(err)
	}
	//rows := []gumshoe.RowMap{
	//{"at": 1.0, "foo": "a", "bar": 123.0},
	//{"at": 1.0, "foo": "a", "bar": 10.0},
	//{"at": 1.0, "foo": "b", "bar": 234.0},
	//}
	//if err := db.Insert(rows); err != nil {
	//log.Fatal(err)
	//}
	//db.Flush()

	fmt.Printf("Compression ratio: %.2f\n", db.GetCompressionRatio())

	const query = `
	{
		"aggregates": [
			{"type": "sum", "column": "bar", "name": "bar"}
		]
	}`
	q, err := db.ParseJSONQuery(strings.NewReader(query))
	if err != nil {
		log.Fatal(err)
	}
	result, err := db.GetQueryResult(q)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\033[01;34m>>>> result: %v\x1B[m\n", result)

	db.Close()
}
