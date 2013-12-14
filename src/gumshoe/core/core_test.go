package core

import "testing"

func TestEndToEnd(t *testing.T) {
	table := NewFactTable()
	populateTableWithTestingData(table)
	invokeQuery(table)
	t.Errorf("yo")
}
