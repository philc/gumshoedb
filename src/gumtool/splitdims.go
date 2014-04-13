package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gumshoe"
)

func init() {
	commandsByName["splitdims"] = command{
		description: "split dimension tables out of db.json and into individual dimension table files",
		fn:          splitDims,
	}
}

type oldDimensionTable struct {
	Values []string
}

type oldDB struct {
	*gumshoe.Schema
	StaticTable struct {
		Intervals       gumshoe.IntervalMap
		DimensionTables []*oldDimensionTable
		Count           int
	}
}

func splitDims(args []string) {
	flags := flag.NewFlagSet("gumtool splitdims", flag.ExitOnError)
	oldDir := flags.String("old_dir", "", "old GumshoeDB database dir")
	newDir := flags.String("new_dir", "", "new GumshoeDB database dir (must not exist)")
	flags.Parse(args)

	if *oldDir == "" || *newDir == "" {
		fatalln("-old_dir and -new_dir are required")
	}

	filename := filepath.Join(*oldDir, gumshoe.MetadataFilename)
	oldDBFile, err := os.Open(filename)
	if err != nil {
		fatalln(err)
	}
	defer oldDBFile.Close()
	decoder := json.NewDecoder(oldDBFile)
	old := new(oldDB)
	if err := decoder.Decode(old); err != nil {
		fatalln(err)
	}

	// Make new DB dir
	if err := os.Mkdir(*newDir, 0755); err != nil {
		fatalln(err)
	}

	// Create new-style dimensions
	dimensionTables := make([]*gumshoe.DimensionTable, len(old.StaticTable.DimensionTables))
	for i, oldDimTable := range old.StaticTable.DimensionTables {
		if oldDimTable == nil {
			continue
		}
		dimensionTables[i] = &gumshoe.DimensionTable{
			Generation: 1,
			Values:     oldDimTable.Values,
		}
	}

	// Write out the new dimension table files
	old.Schema.DiskBacked = true
	old.Schema.Dir = *newDir
	for i, dimTable := range dimensionTables {
		if dimTable == nil {
			continue
		}
		if err := dimTable.Store(old.Schema, i); err != nil {
			fatalln(err)
		}
	}

	// Synthesize a new DB and write it out
	db := &gumshoe.DB{
		Schema: old.Schema,
		StaticTable: &gumshoe.StaticTable{
			Intervals:       old.StaticTable.Intervals,
			DimensionTables: dimensionTables,
			Count:           old.StaticTable.Count,
		},
	}
	newDBFile, err := os.Create(filepath.Join(*newDir, gumshoe.MetadataFilename))
	if err != nil {
		fatalln(err)
	}
	defer newDBFile.Close()
	encoder := json.NewEncoder(newDBFile)
	if err := encoder.Encode(db); err != nil {
		fatalln(err)
	}

	// Copy over interval files
	file, err := filepath.Glob(filepath.Join(*oldDir, "interval.*.dat"))
	if err != nil {
		fatalln(err)
	}
	for _, oldPath := range file {
		name := filepath.Base(oldPath)
		if err := copyFile(filepath.Join(*newDir, name), oldPath); err != nil {
			fatalln(err)
		}
	}

	fmt.Println("Done! Check manually for sanity.")
}

func copyFile(target, source string) error {
	sourceFile, err := os.Open(source)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	stat, err := sourceFile.Stat()
	if err != nil {
		return err
	}
	targetFile, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, stat.Mode())
	if err != nil {
		return err
	}
	defer targetFile.Close()

	_, err = io.Copy(targetFile, sourceFile)
	return err
}
