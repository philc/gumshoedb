package main

import (
	"encoding/json"
	"html/template"
	"time"

	humanize "github.com/dustin/go-humanize"
)

type NameAndCount struct {
	Name  string
	Count int
}

type Stats struct {
	Segments         int
	Rows             int
	Size             uint64  // uint64 for humanize
	CompressionRatio float64 // unused for interval stats
}

type Metricz struct {
	Schema              string
	DimensionTableSizes []NameAndCount
	Totals              Stats
	IntervalStats       map[time.Time]Stats
}

func (s *Server) makeMetricz() (*Metricz, error) {
	schema := s.DB.Schema
	schemaBytes, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return nil, err
	}

	compressionRatio := s.DB.GetCompressionRatio()

	resp := s.DB.MakeRequest()
	defer resp.Done()

	var dimTableSizes []NameAndCount
	for i, col := range schema.DimensionColumns {
		if col.String {
			count := len(resp.State.DimensionTables[i].Values)
			dimTableSizes = append(dimTableSizes, NameAndCount{col.Name, count})
		}
	}

	var totals Stats
	intervalStats := make(map[time.Time]Stats)
	for t, interval := range resp.State.Intervals {
		totals.Segments += interval.NumSegments
		totals.Rows += interval.NumRows
		var size uint64
		for _, segment := range interval.Segments {
			size += uint64(len(segment.Bytes))
		}
		totals.Size += size
		intervalStats[t] = Stats{Segments: interval.NumSegments, Rows: interval.NumRows, Size: size}
	}
	totals.CompressionRatio = compressionRatio

	return &Metricz{
		Schema:              string(schemaBytes),
		DimensionTableSizes: dimTableSizes,
		Totals:              totals,
		IntervalStats:       intervalStats,
	}, nil
}

var funcMap = template.FuncMap{
	"humanize": humanize.Bytes,
	"date":     func(t time.Time) string { return t.Format("2006-01-02 15:04:05") },
}

var metriczTemplate = template.Must(template.New("metricz").Funcs(funcMap).Parse(metriczTemplateText))

const metriczTemplateText = `
<html>
<head>
<title>metricz</title>
<link href='http://fonts.googleapis.com/css?family=Inconsolata:400,700' rel='stylesheet' type='text/css'>
<style>

* { padding: 0; margin: 0; }
body {
	font: 14px Inconsolata;
	width: 900px;
	margin: 40px auto;
}
h1 {
	text-align: center;
	font-size: 24px;
	margin-bottom: 12px;
}
h2 {
	font-size: 18px;
	margin-bottom: 10px;
}
pre {
	font: 14px Inconsolata;
}
table {
	font: 14px Inconsolata;
	border-collapse: collapse;
	width: 100%;
	margin-bottom: 30px;
}
tr:nth-child(even) { background-color: #f3f3f3; }
th,td:first-child { padding-left: 2px; }
th,td:last-child { padding-right: 2px; }
th,td { text-align: center; }
th:first-child,td:first-child { text-align: left; }
th:last-child,td:last-child { text-align: right; }

.columns {
	display: flex;
	width: 100%;
}

.column:first-child {
	width: 300px;
}
.column:last-child {
	flex: 1;
}

</style>
</head>
<body>
<h1>metricz</h1>

<section class="columns">

<section class="column">
<h2>schema</h2>
<pre>
{{.Schema}}
<pre>
</section>

<section class="column">
<h2>Dimension table sizes</h2>
<table>
<tr><th>Name</th><th>Size</th></tr>
{{range .DimensionTableSizes}}
<tr><td>{{.Name}}</td><td>{{.Count}}</td></tr>
{{end}}
</table>

<h2>Stat totals</h2>
{{with .Totals}}
<table>
<tr><th>Segments</th><th>Rows</th><th>Size</th><th>Compression Ratio</th></tr>
<tr><td>{{.Segments}}</td><td>{{.Rows}}</td><td>{{.Size | humanize}}</td><td>{{.CompressionRatio | printf "%.2f"}}</td></tr>
</table>
{{end}}

<h2>Intervals ({{.IntervalStats | len}})</h2>
<table>
<tr><th>Start</th><th>Segments</th><th>Rows</th><th>Size</th></tr>
{{range $start, $stats := .IntervalStats}}
<tr><td>{{$start | date}}</td><td>{{$stats.Segments}}</td><td>{{$stats.Rows}}</td><td>{{$stats.Size | humanize}}</td></tr>
{{end}}
</table>
</section>

</section>

</body>
</html>
`
