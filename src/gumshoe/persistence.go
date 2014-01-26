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

	mmap "github.com/edsrzf/mmap-go"
)

func factsDataFilePath(tableFilePath string) string     { return tableFilePath + ".facts.dat" }
func tableMetadataFilePath(tableFilePath string) string { return tableFilePath + ".json" }
func tmpFilePath(filePath string) string                { return filePath + ".tmp" }

// Creates a new file on disk which is large enough to hold the entire facts table, and maps that file
// into memory to be accessed and modified.
func CreateMemoryMappedFactTableStorage(tableFilePath string, sizeInBytes int) (mmap.MMap, []byte) {
	file, err := os.Create(factsDataFilePath(tableFilePath))
	if err != nil {
		panic(err)
	}
	// Write a single byte at the end of the file to establish its size.
	_, err = file.WriteAt([]byte{0}, int64(sizeInBytes-1))
	if err != nil {
		panic(err)
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
	table.memoryMap, table.rows = memoryMapFactRows(factsDataFilePath(tableFilePath))
	return &table, nil
}

func memoryMapFactRows(filename string) (mmap.MMap, []byte) {
	file, err := os.OpenFile(filename, os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	mmap, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		panic(err)
	}
	if err := file.Close(); err != nil {
		panic(err)
	}
	if err := mmap.Flush(); err != nil {
		panic(err)
	}
	mmapAsByteSlice := []byte(mmap)
	return mmap, mmapAsByteSlice
}

// Persist this database to disk. This blocks until all table metadata has been written, and until the memory
// map has finished being synced.
func (table *FactTable) SaveToDisk() {
	file, err := os.Create(tmpFilePath(tableMetadataFilePath(table.FilePath)))
	if err != nil {
		panic(err)
	}

	bytesBuffer, err := json.Marshal(table)
	if err != nil {
		panic(err)
	}

	file.Write(bytesBuffer)
	file.Close()

	// TODO(caleb): Add real error handling
	// Sync the memory map to disk synchronously.
	if err := table.memoryMap.Flush(); err != nil {
		panic(err)
	}

	err = os.Rename(tmpFilePath(tableMetadataFilePath(table.FilePath)), tableMetadataFilePath(table.FilePath))
	if err != nil {
		panic(err)
	}
}
