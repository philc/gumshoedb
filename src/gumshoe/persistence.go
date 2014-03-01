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
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"unsafe"

	mmap "github.com/edsrzf/mmap-go"
	"time"
)

func factsDataFilePath(tableFilePath string) string     { return tableFilePath + "/facts.dat" }
func tableMetadataFilePath(tableFilePath string) string { return tableFilePath + "/metadata.json" }
func tmpFilePath(filePath string) string                { return filePath + ".tmp" }
func segmentFilePath(tableFilePath string, timestamp time.Time, segmentIndex int) string {
	return fmt.Sprintf("%s/facts.%s.%d.dat", tableFilePath, timestamp.Format("2006-01-02T15:04:05"),
		segmentIndex)
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
	for _, interval := range table.Intervals {
		interval.Segments = make([][]byte, interval.SegmentCount)
		for i := 0; i < interval.SegmentCount; i++ {
			interval.Segments[i] = memoryMapSegment(tableFilePath, interval.Start, i)
		}
	}

	table.FilePath = tableFilePath
	return &table, nil
}

// Persist this database to disk. This blocks until all table metadata has been written, and until the memory
// maps have finished being synced.
func (table *FactTable) SaveToDisk() {
	os.MkdirAll(table.FilePath, 0770)

	file, err := os.Create(tmpFilePath(tableMetadataFilePath(table.FilePath)))
	if err != nil {
		panic(err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(table); err != nil {
		panic(err)
	}

	for _, interval := range table.Intervals {
		for _, segment := range interval.Segments {
			// The segment byte slice is really an mmap.MMap type.
			m := *(*mmap.MMap)(unsafe.Pointer(&segment))
			if err := m.Flush(); err != nil {
				// TODO(caleb): Add real error handling
				panic(err)
			}
		}
	}

	err = os.Rename(tmpFilePath(tableMetadataFilePath(table.FilePath)), tableMetadataFilePath(table.FilePath))
	if err != nil {
		panic(err)
	}
}

// Creates a file on disk and returns the mapped memory.
func createMemoryMappedSegment(tableFilePath string, timestamp time.Time, segmentIndex int, size int) []byte {
	os.MkdirAll(tableFilePath, 0770)
	segmentPath := segmentFilePath(tableFilePath, timestamp, segmentIndex)
	if err := createFileOfSize(segmentPath, size); err != nil {
		panic(err)
	}
	return memoryMapSegment(tableFilePath, timestamp, segmentIndex)
}

// Loads the memory mapped file for the given segment.
func memoryMapSegment(tableFilePath string, timestamp time.Time, segmentIndex int) []byte {
	filename := segmentFilePath(tableFilePath, timestamp, segmentIndex)
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
	return []byte(mmap)
}

// Creates an empty file of the given size.
func createFileOfSize(filename string, sizeInBytes int) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	// Write a single byte at the end of the file to establish its size.
	_, err = file.WriteAt([]byte{0}, int64(sizeInBytes-1))
	if err != nil {
		return err
	}
	file.Close()
	return nil
}
