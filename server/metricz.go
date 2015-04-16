package main

import (
	"encoding/json"
	"html/template"
	"sort"
	"time"

	"github.com/philc/gumshoedb/gumshoe"

	humanize "github.com/philc/gumshoedb/internal/github.com/dustin/go-humanize"
)

type NameAndCount struct {
	Name  string
	Count int
}

type IntervalStatsAndTime struct {
	Time time.Time
	*gumshoe.IntervalStats
}

type byTime []IntervalStatsAndTime

func (b byTime) Len() int           { return len(b) }
func (b byTime) Less(i, j int) bool { return b[i].Time.Before(b[j].Time) }
func (b byTime) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

type Metricz struct {
	Config               string
	DimensionTableCounts []NameAndCount
	Stats                *gumshoe.StaticTableStats
	// Use a slice here so we can show the intervals in order (recent first).
	IntervalStats []IntervalStatsAndTime
}

func (s *Server) makeMetricz() (*Metricz, error) {
	configBytes, err := json.MarshalIndent(s.Config, "", "  ")
	if err != nil {
		return nil, err
	}

	stats := s.DB.GetDebugStats()
	var intervalStats []IntervalStatsAndTime
	for t, is := range stats.ByInterval {
		intervalStats = append(intervalStats, IntervalStatsAndTime{t, is})
	}
	sort.Sort(sort.Reverse(byTime(intervalStats)))

	resp := s.DB.MakeRequest()
	defer resp.Done()

	var dimTableCounts []NameAndCount
	for i, col := range s.DB.Schema.DimensionColumns {
		if col.String {
			count := len(resp.StaticTable.DimensionTables[i].Values)
			dimTableCounts = append(dimTableCounts, NameAndCount{col.Name, count})
		}
	}

	return &Metricz{
		Config:               string(configBytes),
		DimensionTableCounts: dimTableCounts,
		Stats:                stats,
		IntervalStats:        intervalStats,
	}, nil
}

var funcMap = template.FuncMap{
	"humanize": func(size int) string { return humanize.Bytes(uint64(size)) },
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
<h2>Config</h2>
<pre>
{{.Config}}
<pre>
</section>

<section class="column">
<h2>Dimension table counts</h2>
<table>
<tr><th>Name</th><th>Count</th></tr>
{{range .DimensionTableCounts}}
<tr><td>{{.Name}}</td><td>{{.Count}}</td></tr>
{{end}}
</table>

<h2>Stat totals</h2>
{{with .Stats}}
<table>
<tr><th>Segments</th><th>Rows</th><th>Size</th><th>Compression Ratio</th></tr>
<tr><td>{{.Segments}}</td><td>{{.Rows}}</td><td>{{.Bytes | humanize}}</td><td>{{.CompressionRatio | printf "%.2f"}}</td></tr>
</table>
{{end}}

<h2>Intervals ({{.IntervalStats | len}})</h2>
<table>
<tr><th>Start</th><th>Segments</th><th>Rows</th><th>Size</th></tr>
{{range $stats := .IntervalStats}}
<tr><td>{{$stats.Time | date}}</td><td>{{$stats.Segments}}</td><td>{{$stats.Rows}}</td><td>{{$stats.Bytes | humanize}}</td></tr>
{{end}}
</table>
</section>

</section>

</body>
</html>
`
