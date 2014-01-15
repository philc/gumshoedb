// Functions for loading and saving tables to disk.
// A fact table consists of two files:
// 1. tableName.json: The json-encoded table metadata and dimension tables.
// 2. tableName.facts.dat: The facts matrix, encoded as raw bytes, which are memorye-mapped at runtime.
//
// NOTE(philc): I would use gobs for serializing the table metadata and dimension tables, but I had trouble
// getting gobs to fleds on a struct which are maps when serializing the top-level FactTable struct.
package core

import (
	"encoding/json"
	"gommap"
	"io/ioutil"
	"os"
	"reflect"
	"unsafe"
)

func factsDataFilePath(tableFilePath string) string {
	return tableFilePath + ".facts.dat"
}

func tableMetadataFilePath(tableFilePath string) string {
	return tableFilePath + ".json"
}

// Creates a new file on disk which is large enough to hold the entire facts table, and maps that file
// into memory to be accessed and modified.
func CreateMemoryMappedFactTableStorage(tableFilePath string, rows int) (*gommap.MMap, *[ROWS]FactRow) {
	file, err := os.Create(factsDataFilePath(tableFilePath))
	if err != nil {
		panic(err)
	}
	tableSizeInBytes := rows * int(unsafe.Sizeof(FactRow{}))
	// Write a single byte at the end of the file to establish its size.
	_, err = file.WriteAt([]byte{0}, int64(tableSizeInBytes-1))
	if err != nil {
		panic(err)
	}
	file.Close()
	return memoryMapFactRows(factsDataFilePath(tableFilePath))
}

// Load a FactTable from disk. The returned FactTable has its storage memory-mapped to the corresponding
// file on disk.
func LoadFactTableFromDisk(tableFilePath string) *FactTable {
	var table FactTable
	file, err := ioutil.ReadFile(tableMetadataFilePath(tableFilePath))
	if err != nil {
		panic(err)
	}
	json.Unmarshal(file, &table)

	if err != nil {
		panic(err)
	}
	table.memoryMap, table.rows = memoryMapFactRows(factsDataFilePath(tableFilePath))
	return &table
}

func memoryMapFactRows(filename string) (*gommap.MMap, *[ROWS]FactRow) {
	file, err := os.OpenFile(filename, os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	mmap, err := gommap.Map(file.Fd(), gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	file.Close()
	if err != nil {
		panic(err)
	}
	sliceHeader := *(*reflect.SliceHeader)(unsafe.Pointer(&mmap))
	table := (*[ROWS]FactRow)(unsafe.Pointer(sliceHeader.Data))
	err = mmap.Sync(gommap.MS_SYNC)
	if err != nil {
		panic(err)
	}
	return &mmap, table
}

// Persist this database to disk. This blocks until all table metadata has been written, and until the memory
// map has finished being synced.
func (table *FactTable) SaveToDisk() {
	// TODO(philc): Make this writing method safer. If this writing process is interrupted, we lose/corrupt the
	// entire table. Instead, we should serialize the fact table's metadata and dimension tables to a temporary
	// file and then atomically rename it to the destination file.
	file, err := os.Create(tableMetadataFilePath(table.FilePath))
	if err != nil {
		panic(err)
	}

	bytesBuffer, err := json.Marshal(table)
	if err != nil {
		panic(err)
	}

	file.Write(bytesBuffer)
	file.Close()
	table.memoryMap.Sync(gommap.MS_SYNC) // Sync the memory map to disk synchronously.
}
