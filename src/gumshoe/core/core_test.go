package core

import "testing"

func TestEndToEnd(t *testing.T) {
	table := NewFactTable([]string{"at", "country", "impressions", "clicks"})
	populateTableWithTestingData(table)
	invokeQuery(table)
	t.Errorf("fail")
}
