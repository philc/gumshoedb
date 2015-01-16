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
	"strings"
	"time"

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
	IntervalDuration time.Duration
	TimestampColName string
	Shards           []string
	Client           *http.Client
}

// TODO(caleb): This route is totally untested and probably won't work the first time.
func (r *Router) HandleInsert(w http.ResponseWriter, req *http.Request) {
	decoder := json.NewDecoder(req.Body)
	var rows []gumshoe.RowMap
	if err := decoder.Decode(&rows); err != nil {
		WriteError(w, err, http.StatusBadRequest)
		return
	}
	Log.Printf("Inserting %d rows", len(rows))

	shardedRows := make([][]gumshoe.RowMap, len(r.Shards))
	for _, row := range rows {
		timestamp, ok := row[r.TimestampColName]
		if !ok {
			WriteError(w, errors.New("row must have a value for the timestamp column"), http.StatusBadRequest)
			return
		}
		timestampMillis, ok := timestamp.(float64)
		if !ok {
			WriteError(w, errors.New("timestamp column must have a numeric value"), http.StatusBadRequest)
			return
		}
		intervalIdx := int(time.Duration(timestampMillis) * time.Millisecond / r.IntervalDuration)
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
		WriteError(w, err, http.StatusInternalServerError) // TODO(caleb): Better status?
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
			var result Result
			decoder := json.NewDecoder(resp.Body)
			if err := decoder.Decode(&result); err != nil {
				return err
			}
			results[i] = result
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		WriteError(w, err, http.StatusInternalServerError) // TODO(caleb): Better status?
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
				mergeRows(cur, row, query)
				resultMap[groupByValue] = cur
			}
		}
		for _, row := range resultMap {
			finalResult = append(finalResult, row)
		}
	} else {
		finalResult = []gumshoe.RowMap{results[0].Results[0]}
		for _, result := range results[1:] {
			mergeRows(finalResult[0], result.Results[0], query)
		}
	}

	WriteJSONResponse(w, Result{
		Results:    finalResult,
		DurationMS: int(time.Since(start).Seconds() * 1000),
	})
}

// mergeRows merges row2 into row1.
func mergeRows(row1, row2 gumshoe.RowMap, q *gumshoe.Query) {
	for _, agg := range q.Aggregates {
		row1[agg.Name] = sumColumn(row1, row2, agg.Name)
	}
	row1["rowCount"] = sumColumn(row1, row2, "rowCount")
}

func sumColumn(row1, row2 gumshoe.RowMap, col string) float64 {
	// TODO(caleb): Sigh. The neverending scourge of JSON having no integer type.
	// We shouldn't use floats here if the column types are ints. I think the right way to do this is to just
	// load the DB config into the router. That way we get the interval duration and the 'at' column, plus we
	// also get the types for all the metrics which we can make use of here to coerce these results into ints.
	val1, ok := row1[col]
	if !ok {
		val1 = float64(0)
	}
	val2, ok := row2[col]
	if !ok {
		val2 = float64(0)
	}
	return val1.(float64) + val2.(float64)
}

type Result struct {
	Results    []gumshoe.RowMap `json:"results"`
	DurationMS int              `json:"duration_ms"`
}

func (r *Router) HandleUnimplemented(w http.ResponseWriter, req *http.Request) {
	http.Error(w, "unimplemented", http.StatusInternalServerError)
}

func (*Router) HandleRoot(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte("Gumshoe router is alive!"))
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

func NewRouter(shards []string, intervalDuration time.Duration, timestampColName string) *Router {
	r := &Router{
		IntervalDuration: intervalDuration,
		TimestampColName: timestampColName,
		Shards:           shards,
		Client:           &http.Client{}, // TODO(caleb): better non-default settings
	}

	mux := pat.New()

	mux.Put("/insert", r.HandleInsert)
	mux.Get("/dimension_tables/{name}", r.HandleUnimplemented) // TODO(caleb)
	mux.Get("/dimension_tables", r.HandleUnimplemented)        // TODO(caleb)
	mux.Post("/query", r.HandleQuery)

	mux.Get("/metricz", r.HandleUnimplemented)
	mux.Get("/debug/rows", r.HandleUnimplemented)
	mux.Get("/statusz", r.HandleUnimplemented) // TODO(caleb)
	mux.Get("/", r.HandleRoot)

	r.Handler = apachelog.NewDefaultHandler(mux)
	return r
}

func main() {
	intervalDuration := flag.Duration("interval-duration", 0, "interval size (should match shard configs)")
	timestampColName := flag.String("timestamp-column-name", "at", "name of the timestamp column")
	shardsFlag := flag.String("shards", "", "comma-separated list of shard addresses (with ports)")
	listenAddr := flag.String("addr", "localhost:9090", "address on which to listen")
	flag.Parse()
	if *intervalDuration == 0 {
		log.Fatal("-interval-duration must be provided")
	}
	shardAddrs := strings.Split(*shardsFlag, ",")
	if len(shardAddrs) == 0 {
		log.Fatal("At least one shard required")
	}

	r := NewRouter(shardAddrs, *intervalDuration, *timestampColName)
	server := &http.Server{
		Addr:    *listenAddr,
		Handler: r,
	}
	Log.Println("Now serving on", *listenAddr)
	Log.Fatal(server.ListenAndServe())
}
