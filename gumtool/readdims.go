package main

import (
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"os"
	"strings"
)

func init() {
	commandsByName["readdims"] = command{
		description: "read a dimension file and write strings to stdout",
		fn:          readDims,
	}
}

func readDims(args []string) {
	if len(args) != 1 {
		fatalln("expected 1 filename")
	}
	filename := args[0]
	if !strings.HasSuffix(filename, ".gob.gz") {
		fatalln("expected file to be named *.gob.gz")
	}
	f, err := os.Open(filename)
	if err != nil {
		fatalln(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fatalln(err)
	}
	defer gz.Close()
	var vals []string
	decoder := gob.NewDecoder(gz)
	if err := decoder.Decode(&vals); err != nil {
		fatalln("could not decode dimension values:", err)
	}
	for _, val := range vals {
		fmt.Println(val)
	}
}
