package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"gumshoe"
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
	decoder := json.NewDecoder(f)
	db := new(gumshoe.DB)
	if err := decoder.Decode(db); err != nil {
		fatalln(err)
	}

	existingFiles := make(map[string]bool)
	expectedFiles := make(map[string]bool)

	existing, err := filepath.Glob(filepath.Join(*dir, "interval.*.dat"))
	if err != nil {
		fatalln(err)
	}
	for _, filename := range existing {
		existingFiles[filepath.Base(filename)] = true
	}

	for _, interval := range db.StaticTable.Intervals {
		for i := 0; i < interval.NumSegments; i++ {
			expectedFiles[interval.SegmentFilename(db.Schema, i)] = true
		}
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
		fmt.Printf("Warning: missing %d database file(s):\n", len(missingFiles))
		for _, filename := range missingFiles {
			fmt.Printf("  %s\n", filename)
		}
		fmt.Println()
	}

	if len(extraFiles) == 0 {
		fmt.Println("No extra files found.")
		return
	}

	fmt.Printf("Found %d extra file(s) not referenced by DB metadata:\n", len(extraFiles))
	for _, filename := range extraFiles {
		fmt.Printf("  %s\n", filename)
	}
	fmt.Printf("Delete these %d file(s)s? [y/N]: ", len(extraFiles))
	var response string
	fmt.Scanln(&response)
	switch response {
	case "y", "Y", "yes":
	default:
		fmt.Println("Not deleting files.")
		return
	}

	for _, filename := range extraFiles {
		if err := os.Remove(filepath.Join(*dir, filename)); err != nil {
			fatalln(err)
		}
	}
	fmt.Printf("%d extra files deleted successfully.\n", len(extraFiles))
}
