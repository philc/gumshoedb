package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/philc/gumshoedb/gumshoe"
)

func init() {
	commandsByName["clean"] = command{
		description: "repair a GumshoeDB database that was stopped in an inconsistent state",
		fn:          clean,
	}
}

func clean(args []string) {
	flags := flag.NewFlagSet("gumtool clean", flag.ExitOnError)
	dir := flags.String("dir", "", "the GumshoeDB database directory for cleaning")
	flags.Parse(args)

	if *dir == "" {
		fatalln("-dir must be provided")
	}

	filename := filepath.Join(*dir, gumshoe.MetadataFilename)
	f, err := os.Open(filename)
	if err != nil {
		fatalln(err)
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	db := new(gumshoe.DB)
	if err := decoder.Decode(db); err != nil {
		fatalln(err)
	}

	var expectedDimensionFiles []string
	for i, dimTable := range db.StaticTable.DimensionTables {
		if dimTable == nil || dimTable.Generation == 0 {
			continue
		}
		expectedDimensionFiles = append(expectedDimensionFiles, dimTable.Filename(db.Schema, i))
	}

	var expectedIntervalFiles []string
	for _, interval := range db.StaticTable.Intervals {
		for i := 0; i < interval.NumSegments; i++ {
			expectedIntervalFiles = append(expectedIntervalFiles, interval.SegmentFilename(db.Schema, i))
		}
	}

	warnMissingAndRemoveExtras(expectedDimensionFiles, "dimension table", *dir, "dimension.*.gob.gz")
	warnMissingAndRemoveExtras(expectedIntervalFiles, "interval", *dir, "interval.*.dat")
}

func warnMissingAndRemoveExtras(expected []string, typeDescription, dir, glob string) {
	existingFiles := make(map[string]bool)
	expectedFiles := make(map[string]bool)

	for _, filename := range expected {
		expectedFiles[filename] = true
	}

	existing, err := filepath.Glob(filepath.Join(dir, glob))
	if err != nil {
		fatalln(err)
	}
	for _, filename := range existing {
		existingFiles[filepath.Base(filename)] = true
	}

	var missingFiles, extraFiles []string

	for filename := range expectedFiles {
		if !existingFiles[filename] {
			missingFiles = append(missingFiles, filename)
		}
		delete(existingFiles, filename)
	}
	for filename := range existingFiles {
		extraFiles = append(extraFiles, filename)
	}

	if len(missingFiles) > 0 {
		fmt.Printf("Warning: missing %d %s file(s):\n", len(missingFiles), typeDescription)
		for _, filename := range missingFiles {
			fmt.Printf("  %s\n", filename)
		}
		fmt.Println()
	}

	if len(extraFiles) == 0 {
		fmt.Printf("No extra %s file(s) found.\n", typeDescription)
		return
	}

	fmt.Printf("Found %d extra %s file(s) not referenced by DB metadata:\n", len(extraFiles), typeDescription)
	for _, filename := range extraFiles {
		fmt.Printf("  %s\n", filename)
	}
	fmt.Printf("Delete these %d %s file(s)s? [y/N]: ", len(extraFiles), typeDescription)
	var response string
	fmt.Scanln(&response)
	switch response {
	case "y", "Y", "yes":
	default:
		fmt.Println("Not deleting files.")
		return
	}

	for _, filename := range extraFiles {
		if err := os.Remove(filepath.Join(dir, filename)); err != nil {
			fatalln(err)
		}
	}
	fmt.Printf("%d extra %s file(s) deleted successfully.\n", len(extraFiles), typeDescription)
}
