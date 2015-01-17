package main

// How to run:
//
// $ glp build router
// $ ./router -interval-duration 1h -shards 'host1:8080,host2:8080'
//
// (or just test with -shards 'localhost:8080,localhost:8080' and verify you get 2x results)

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"config"
	"gumshoe"

	"github.com/cespare/hutil/apachelog"
	"github.com/cespare/wait"
	"github.com/gorilla/pat"
)

const logFlags = log.Lshortfile

var (
	Log = log.New(os.Stderr, "[router] ", logFlags)
)

type Router struct {
	http.Handler
	Schema *gumshoe.Schema
	Shards []string
	Client *http.Client
}

func (r *Router) HandleInsert(w http.ResponseWriter, req *http.Request) {
	var rows []gumshoe.RowMap
	if err := json.NewDecoder(req.Body).Decode(&rows); err != nil {
		WriteError(w, err, http.StatusBadRequest)
		return
	}
	Log.Printf("Inserting %d rows", len(rows))

	shardedRows := make([][]gumshoe.RowMap, len(r.Shards))
	for _, row := range rows {
		timestamp, ok := row[r.Schema.TimestampColumn.Name]
		if !ok {
			WriteError(w, errors.New("row must have a value for the timestamp column"), http.StatusBadRequest)
			return
		}
		timestampUnix, ok := timestamp.(float64)
		if !ok {
			WriteError(w, errors.New("timestamp column must have a numeric value"), http.StatusBadRequest)
			return
		}
		// Check that the columns match the schema we have
		for col := range row {
			if !r.validColumnName(col) {
				writeInvalidColumnError(w, col)
				return
			}
		}
		intervalIdx := int(time.Duration(timestampUnix) * time.Second / r.Schema.IntervalDuration)
		shardIdx := intervalIdx % len(r.Shards)
		shardedRows[shardIdx] = append(shardedRows[shardIdx], row)
	}
	var wg wait.Group
	for i := range shardedRows {
		i := i
		wg.Go(func(_ <-chan struct{}) error {
			shard := r.Shards[i]
			b, err := json.Marshal(shardedRows[i])
			if err != nil {
				panic("unexpected marshal error")
			}
			shardReq, err := http.NewRequest("PUT", "http://"+shard+"/insert", bytes.NewReader(b))
			if err != nil {
				panic("could not make http request")
			}
			shardReq.Header.Set("Content-Type", "application/json")
			resp, err := r.Client.Do(shardReq)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				return fmt.Errorf("non-200 response from shard %s: %d", shard, resp.StatusCode)
			}
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		WriteError(w, err, http.StatusInternalServerError)
	}
}

func (r *Router) HandleQuery(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	query, err := gumshoe.ParseJSONQuery(req.Body)
	if err != nil {
		WriteError(w, err, http.StatusBadRequest)
		return
	}
	for _, agg := range query.Aggregates {
		if agg.Type == gumshoe.AggregateAvg {
			// TODO(caleb): Handle as described in the doc.
			WriteError(w, errors.New("average aggregates not handled by the router"), http.StatusInternalServerError)
			return
		}
		if !r.validColumnName(agg.Column) {
			writeInvalidColumnError(w, agg.Column)
			return
		}
	}
	for _, grouping := range query.Groupings {
		if !r.validColumnName(grouping.Column) {
			writeInvalidColumnError(w, grouping.Column)
			return
		}
	}
	for _, filter := range query.Filters {
		if !r.validColumnName(filter.Column) {
			writeInvalidColumnError(w, filter.Column)
			return
		}
	}
	b, err := json.Marshal(query)
	if err != nil {
		panic("unexpected marshal error")
	}
	var wg wait.Group
	results := make([]Result, len(r.Shards))
	for i := range r.Shards {
		i := i
		wg.Go(func(_ <-chan struct{}) error {
			resp, err := r.Client.Post("http://"+r.Shards[i]+"/query", "application/json", bytes.NewReader(b))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			var result Result
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return err
			}
			results[i] = result
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		WriteError(w, err, http.StatusInternalServerError)
		return
	}

	var finalResult []gumshoe.RowMap
	if len(query.Groupings) > 0 {
		resultMap := make(map[interface{}]gumshoe.RowMap)
		for _, result := range results {
			for _, row := range result.Results {
				groupByValue := row[query.Groupings[0].Name]
				cur := resultMap[groupByValue]
				if cur == nil {
					cur = make(gumshoe.RowMap)
					cur[query.Groupings[0].Name] = groupByValue
				}
				r.mergeRows(cur, row, query)
				resultMap[groupByValue] = cur
			}
		}
		for _, row := range resultMap {
			finalResult = append(finalResult, row)
		}
	} else {
		finalResult = []gumshoe.RowMap{results[0].Results[0]}
		for _, result := range results[1:] {
			r.mergeRows(finalResult[0], result.Results[0], query)
		}
	}

	WriteJSONResponse(w, Result{
		Results:    finalResult,
		DurationMS: int(time.Since(start).Seconds() * 1000),
	})
}

// mergeRows merges row2 into row1.
func (r *Router) mergeRows(row1, row2 gumshoe.RowMap, q *gumshoe.Query) {
	for _, agg := range q.Aggregates {
		row1[agg.Name] = r.sumColumn(row1, row2, agg.Column)
	}
	row1["rowCount"] = r.sumColumn(row1, row2, "rowCount")
}

// sumColumn figures out the appropriate types and sums the column from row1 and row2 using either int64s or
// float64s.
func (r *Router) sumColumn(row1, row2 gumshoe.RowMap, col string) interface{} {
	// NOTE(caleb): this function is pretty gross because of the JSON interface here. This should get cleaned up
	// when we do a general JSON purge of gumshoeDB.
	val1, ok := row1[col]
	if !ok {
		val1 = float64(0)
	}
	val2, ok := row2[col]
	if !ok {
		val2 = float64(0)
	}

	var typ gumshoe.Type
	if col == "rowCount" {
		typ = gumshoe.TypeInt64
	} else if i, ok := r.Schema.DimensionNameToIndex[col]; ok {
		typ = r.Schema.DimensionColumns[i].Type
	} else if i, ok := r.Schema.MetricNameToIndex[col]; ok {
		typ = r.Schema.MetricColumns[i].Type
	} else {
		panic("bad column in sumColumn: " + col)
	}
	switch typ {
	case gumshoe.TypeUint8, gumshoe.TypeInt8, gumshoe.TypeUint16, gumshoe.TypeInt16, gumshoe.TypeUint32, gumshoe.TypeInt32, gumshoe.TypeUint64, gumshoe.TypeInt64:
		switch v1 := val1.(type) {
		case int64:
			return v1 + int64(val2.(float64))
		case float64:
			return int64(v1) + int64(val2.(float64))
		}
	case gumshoe.TypeFloat32, gumshoe.TypeFloat64:
		return val1.(float64) + val2.(float64)
	}
	panic("unexpected type")
}

type Result struct {
	Results    []gumshoe.RowMap `json:"results"`
	DurationMS int              `json:"duration_ms"`
}

func (r *Router) HandleSingleDimension(w http.ResponseWriter, req *http.Request) {
	name := req.URL.Query().Get(":name")
	if name == "" {
		http.Error(w, "must provide dimension name", http.StatusBadRequest)
		return
	}

	var wg wait.Group
	dimValues := make(map[string]struct{})
	var mu sync.Mutex
	for i := range r.Shards {
		i := i
		wg.Go(func(_ <-chan struct{}) error {
			resp, err := r.Client.Get("http://" + r.Shards[i] + "/dimension_tables/" + name)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			var result []string
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return err
			}
			mu.Lock()
			for _, s := range result {
				dimValues[s] = struct{}{}
			}
			mu.Unlock()
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		WriteError(w, err, http.StatusInternalServerError)
		return
	}

	var results []string
	for s := range dimValues {
		results = append(results, s)
	}
	sort.Strings(results)
	WriteJSONResponse(w, results)
}

type Statusz struct {
	LastUpdated    *int64
	OldestInterval *int64
}

func (r *Router) HandleStatusz(w http.ResponseWriter, req *http.Request) {
	var status Statusz
	var failed []string
	for _, shard := range r.Shards {
		resp, err := r.Client.Get("http://" + shard + "/statusz")
		if err != nil {
			failed = append(failed, shard)
			continue
		}
		defer resp.Body.Close()
		var status2 Statusz
		if err := json.NewDecoder(resp.Body).Decode(&status2); err != nil {
			failed = append(failed, shard)
			continue
		}
		if status.LastUpdated == nil ||
			(status2.LastUpdated != nil && *status2.LastUpdated > *status.LastUpdated) {
			status.LastUpdated = status2.LastUpdated
		}
		if status.OldestInterval == nil ||
			(status2.OldestInterval != nil && *status2.OldestInterval < *status.OldestInterval) {
			status.OldestInterval = status2.OldestInterval
		}
	}
	if len(failed) > 0 {
		var buf bytes.Buffer
		fmt.Fprintln(&buf, "Could not contact shards:")
		for _, shard := range failed {
			fmt.Fprintln(&buf, shard)
		}
		WriteError(w, errors.New(buf.String()), http.StatusInternalServerError)
		return
	}
	WriteJSONResponse(w, status)
}

func (r *Router) HandleUnimplemented(w http.ResponseWriter, req *http.Request) {
	http.Error(w, "this route is not implemented in gumshoe router", http.StatusInternalServerError)
}

func (r *Router) HandleRoot(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintln(w, "shards:")
	for _, shard := range r.Shards {
		fmt.Fprintln(w, shard)
	}

}

func WriteJSONResponse(w http.ResponseWriter, objectToSerialize interface{}) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(objectToSerialize); err != nil {
		WriteError(w, err, 500)
		return
	}
}

func WriteError(w http.ResponseWriter, err error, status int) {
	Log.Print(err)
	http.Error(w, err.Error(), status)
}

func (r *Router) validColumnName(name string) bool {
	if name == r.Schema.TimestampColumn.Name {
		return true
	}
	if _, ok := r.Schema.DimensionNameToIndex[name]; ok {
		return true
	}
	_, ok := r.Schema.MetricNameToIndex[name]
	return ok
}

func writeInvalidColumnError(w http.ResponseWriter, name string) {
	WriteError(w, fmt.Errorf("%q is not a valid column name", name), http.StatusBadRequest)
}

func NewRouter(shards []string, schema *gumshoe.Schema) *Router {
	transport := &http.Transport{MaxIdleConnsPerHost: 8}
	r := &Router{
		Schema: schema,
		Shards: shards,
		Client: &http.Client{Transport: transport},
	}

	mux := pat.New()

	mux.Put("/insert", r.HandleInsert)
	mux.Get("/dimension_tables/{name}", r.HandleSingleDimension)
	mux.Get("/dimension_tables", r.HandleUnimplemented)
	mux.Post("/query", r.HandleQuery)

	mux.Get("/metricz", r.HandleUnimplemented)
	mux.Get("/debug/rows", r.HandleUnimplemented)
	mux.Get("/statusz", r.HandleStatusz)
	mux.Get("/", r.HandleRoot)

	r.Handler = apachelog.NewDefaultHandler(mux)
	return r
}

func main() {
	configFile := flag.String("config", "config.toml", "path to a DB config (to get the schema)")
	shardsFlag := flag.String("shards", "", "comma-separated list of shard addresses (with ports)")
	port := flag.Int("port", 9090, "port on which to listen")
	flag.Parse()
	shardAddrs := strings.Split(*shardsFlag, ",")
	if *shardsFlag == "" || len(shardAddrs) == 0 {
		Log.Fatal("At least one shard required")
	}
	f, err := os.Open(*configFile)
	if err != nil {
		Log.Fatal(err)
	}
	defer f.Close()
	_, schema, err := config.LoadTOMLConfig(f)
	if err != nil {
		Log.Fatal(err)
	}
	schema.Initialize()

	r := NewRouter(shardAddrs, schema)
	addr := fmt.Sprintf(":%d", *port)
	server := &http.Server{
		Addr:    addr,
		Handler: r,
	}
	Log.Println("Now serving on", addr)
	Log.Fatal(server.ListenAndServe())
}
