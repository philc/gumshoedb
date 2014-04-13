package gumshoe

import (
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
)

type DimensionTable struct {
	Generation   int
	Values       []string          `json:"-"`
	ValueToIndex map[string]uint32 `json:"-"`
}

func newDimensionTable(generation int, values []string) *DimensionTable {
	valueToIndex := make(map[string]uint32)
	for i, value := range values {
		valueToIndex[value] = uint32(i)
	}
	return &DimensionTable{
		Generation:   generation,
		Values:       values,
		ValueToIndex: valueToIndex,
	}
}

func NewDimensionTablesForSchema(schema *Schema) []*DimensionTable {
	dimTables := make([]*DimensionTable, len(schema.DimensionColumns))
	for i, col := range schema.DimensionColumns {
		if col.String {
			dimTables[i] = &DimensionTable{ValueToIndex: make(map[string]uint32)}
		}
	}
	return dimTables
}

func (t *DimensionTable) Get(s string) (index uint32, ok bool) {
	i, ok := t.ValueToIndex[s]
	return i, ok
}

func (t *DimensionTable) GetAndMaybeSet(s string) (index uint32, alreadyExisted bool) {
	i, ok := t.ValueToIndex[s]
	if !ok {
		i = uint32(len(t.Values))
		t.ValueToIndex[s] = i
		t.Values = append(t.Values, s)
	}
	return i, ok
}

// Filename returns the filename for this dimension index, which includes the dimension table index and
// generation and is located in the schema directory.
func (t *DimensionTable) Filename(s *Schema, index int) string {
	return filepath.Join(s.Dir, fmt.Sprintf("dimension.index%d.generation%d.gob.gz", index, t.Generation))
}

// Load reads a dimension table file identified by the schema directory, this dimension table's index, and the
// table generation and loads it into t. t.Values and t.ValuesToIndex are overwritten.
func (t *DimensionTable) Load(s *Schema, index int) error {
	f, err := os.Open(t.Filename(s, index))
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	decoder := gob.NewDecoder(gz)
	if err := decoder.Decode(&t.Values); err != nil {
		return err
	}
	t.ValueToIndex = make(map[string]uint32)
	for i, value := range t.Values {
		t.ValueToIndex[value] = uint32(i)
	}
	return nil
}

// Store writes this dimension table to a new file identified by the schema directory, the provided dimension
// table index, and the table generation. It is an error if the file already exists.
func (t *DimensionTable) Store(s *Schema, index int) error {
	f, err := os.OpenFile(t.Filename(s, index), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	defer gz.Close()
	encoder := gob.NewEncoder(gz)
	return encoder.Encode(t.Values)
}
