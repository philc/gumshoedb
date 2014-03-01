// Functions for loading and saving tables to disk.
// A fact table consists of two files:
// 1. tableName.json: The json-encoded table metadata and dimension tables.
// 2. tableName.facts.dat: The facts matrix, encoded as raw bytes, which are memory-mapped at runtime.
//
// NOTE(philc): I would use gobs for serializing the table metadata and dimension tables, but I had trouble
// getting gobs to fields on a struct which are maps when serializing the top-level FactTable struct.
package gumshoe

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	mmap "github.com/edsrzf/mmap-go"
)

func factsDataFilePath(tableFilePath string) string     { return tableFilePath + ".facts.dat" }
func tableMetadataFilePath(tableFilePath string) string { return tableFilePath + ".json" }
func tmpFilePath(filePath string) string                { return filePath + ".tmp" }

// Creates a new file on disk which is large enough to hold the entire facts table, and maps that file
// into memory to be accessed and modified.
func CreateMemoryMappedFactTableStorage(tableFilePath string, sizeInBytes int) (mmap.MMap, error) {
	if err := os.MkdirAll(filepath.Dir(tableFilePath), 0755); err != nil {
		return nil, err
	}
	file, err := os.Create(factsDataFilePath(tableFilePath))
	if err != nil {
		return nil, err
	}
	// Write a single byte at the end of the file to establish its size.
	if _, err = file.WriteAt([]byte{0}, int64(sizeInBytes-1)); err != nil {
		return nil, err
	}
	file.Close()
	return memoryMapFactRows(factsDataFilePath(tableFilePath))
}

// Load a FactTable from disk. The returned FactTable has its storage memory-mapped to the corresponding
// file on disk.
func LoadFactTableFromDisk(tableFilePath string) (*FactTable, error) {
	var table FactTable
	file, err := ioutil.ReadFile(tableMetadataFilePath(tableFilePath))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(file, &table); err != nil {
		return nil, err
	}
	table.InsertLock = new(sync.Mutex)
	table.memoryMap, err = memoryMapFactRows(factsDataFilePath(tableFilePath))
	if err != nil {
		return nil, err
	}
	table.rows = []byte(table.memoryMap)
	table.FilePath = tableFilePath
	return &table, nil
}

func memoryMapFactRows(filename string) (mmap.MMap, error) {
	file, err := os.OpenFile(filename, os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	mmap, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	if err := mmap.Flush(); err != nil {
		return nil, err
	}
	return mmap, nil
}

// Persist this database to disk. This blocks until all table metadata has been written, and until the memory
// map has finished being synced.
func (table *FactTable) SaveToDisk() error {
	file, err := os.Create(tmpFilePath(tableMetadataFilePath(table.FilePath)))
	if err != nil {
		return err
	}
	if err := json.NewEncoder(file).Encode(table); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}

	// Sync the memory map to disk synchronously.
	if err := table.memoryMap.Flush(); err != nil {
		return err
	}

	return os.Rename(tmpFilePath(tableMetadataFilePath(table.FilePath)), tableMetadataFilePath(table.FilePath))
}
