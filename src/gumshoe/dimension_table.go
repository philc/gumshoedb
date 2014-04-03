package gumshoe

import (
	"encoding/json"
)

type DimensionTable struct {
	Values       []string
	ValueToIndex map[string]uint32 `json:"-"`
}

func newDimensionTable() *DimensionTable {
	return &DimensionTable{
		ValueToIndex: make(map[string]uint32),
	}
}

func NewDimensionTablesForSchema(schema *Schema) []*DimensionTable {
	dimTables := make([]*DimensionTable, len(schema.DimensionColumns))
	for i, col := range schema.DimensionColumns {
		if col.String {
			dimTables[i] = newDimensionTable()
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

func (d *DimensionTable) UnmarshalJSON(b []byte) error {
	var v struct{ Values []string }
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	d.Values = v.Values
	d.ValueToIndex = make(map[string]uint32)
	for i, value := range d.Values {
		d.ValueToIndex[value] = uint32(i)
	}
	return nil
}
