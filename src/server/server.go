// A web server which runs the database as a daemon, and exposes routes for ingesting rows and executing
// queries.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"
	"unsafe"

	"config"
	"gumshoe"

	"github.com/BurntSushi/toml"
	"github.com/codegangsta/martini"
)

var (
	configFile  = flag.String("config", "config.toml", "Configuration file to use")
	profileAddr = flag.String("profile-addr", "", "If non-empty, address for net/http/pprof")
)

type Server struct {
	http.Handler
	Config *config.Config
	Table  *gumshoe.FactTable
}

func WriteJSONResponse(w http.ResponseWriter, objectToSerialize interface{}) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(objectToSerialize); err != nil {
		log.Print(err)
		http.Error(w, err.Error(), 500)
		return
	}
}

// Given an array of JSON rows, insert them.
func (s *Server) HandleInsertRoute(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	jsonBody := []gumshoe.RowMap{}
	if err := decoder.Decode(&jsonBody); err != nil {
		log.Print(err)
		http.Error(w, err.Error(), 500)
		return
	}
	log.Printf("Inserting %d rows", len(jsonBody))

	if err := s.Table.InsertRowMaps(jsonBody); err != nil {
		log.Print(err)
		http.Error(w, err.Error(), 500)
		return
	}
}

// A debugging route to list the contents of a table. Returns up to 1000 rows.
func (s *Server) HandleFactTableRoute(w http.ResponseWriter, r *http.Request) {
	// For now, only return up to 1000 rows. We can't serialize the entire table unless we stream the response,
	// and for debugging, we only need a few rows to inspect that importing is working correctly.
	maxRowsToReturn := 1000
	rowCount := int(math.Min(float64(s.Table.Count), float64(maxRowsToReturn)))
	WriteJSONResponse(w, s.Table.GetRowMaps(0, rowCount))
}

// Returns the contents of the all of the dimensions tables, for use when debugging.
func (s *Server) HandleDimensionsTableRoute(w http.ResponseWriter, r *http.Request) {
	// Assembles the map: {dimensionTableName => [ [0 value0] [1 value1] ... ]}
	results := make(map[string][][2]gumshoe.Untyped)
	for _, dimensionTable := range s.Table.DimensionTables {
		rows := make([][2]gumshoe.Untyped, 0, s.Table.ColumnCount)
		for i, value := range dimensionTable.Rows {
			row := [2]gumshoe.Untyped{i, value}
			rows = append(rows, row)
		}
		results[dimensionTable.Name] = rows
	}
	WriteJSONResponse(w, results)
}

// Evaluate a query and returns an aggregated result set.
// See the README for the query JSON structure and the structure of the reuslts.
func (s *Server) HandleQueryRoute(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestBody, _ := ioutil.ReadAll(r.Body)
	query, err := gumshoe.ParseJSONQuery(string(requestBody))
	if err != nil {
		log.Print(err)
		http.Error(w, err.Error(), 500)
		return
	}
	if err = gumshoe.ValidateQuery(s.Table, query); err != nil {
		log.Print(err)
		http.Error(w, err.Error(), 500)
		return
	}
	results := s.Table.InvokeQuery(query)
	results["duration"] = (time.Since(start)).Nanoseconds() / (1000.0 * 1000.0)
	WriteJSONResponse(w, results)
}

type Metricz struct {
	FactTableRows          int                        `json:"factTableRows"`
	FactTableBytes         int                        `json:"factTableBytes"`
	FactTableCapacityBytes int                        `json:"factTableCapacityBytes"`
	DimensionTables        map[string]map[string]int  `json:"dimensionTables"`
	OldestRow              map[string]gumshoe.Untyped `json:"oldestRow"`
	NewestRow              map[string]gumshoe.Untyped `json:"newestRow"`
}

func (s *Server) HandleMetricz(w http.ResponseWriter) {
	dimensionTables := make(map[string]map[string]int)
	for _, dimensionTable := range s.Table.DimensionTables {
		if dimensionTable != nil {
			dimensionTables[dimensionTable.Name] = map[string]int{
				"Rows":  len(dimensionTable.Rows),
				"Bytes": len(dimensionTable.Rows) * int(unsafe.Sizeof(*dimensionTable)),
			}
		}
	}

	metricz := &Metricz{
		FactTableRows:   s.Table.Count,
		FactTableBytes:  s.Table.Count * s.Table.RowSize,
		DimensionTables: dimensionTables,
	}

	if s.Table.Count > 0 {
		s.Table.InsertLock.Lock()
		defer s.Table.InsertLock.Unlock()
		metricz.OldestRow = s.Table.GetRowMaps(0, 1)[0]
		metricz.NewestRow = s.Table.GetRowMaps(s.Table.Count-1, s.Table.Count)[0]
	}

	metriczJSON, err := json.Marshal(metricz)

	if err != nil {
		log.Print(err)
		http.Error(w, err.Error(), 500)
	}

	var buf bytes.Buffer
	if err := json.Indent(&buf, metriczJSON, "", "    "); err != nil {
		log.Print(err)
		http.Error(w, err.Error(), 500)
	}
	buf.WriteTo(w)
}

// Loads the fact table from disk if it exists, or creates a new one.
// TODO(caleb): We should probably just do this inside NewServer, but calling separately for now so the test
// can avoid loading a fact table.
func (s *Server) loadFactTable() {
	log.Printf(`Trying to load "%s"... `, s.Config.TableFilePath)
	table, err := gumshoe.LoadFactTableFromDisk(s.Config.TableFilePath)
	if err != nil {
		if !os.IsNotExist(err) {
			// TODO(caleb): handle
			panic(err)
		}
		log.Printf(`Table "%s" does not exist, creating... `, s.Config.TableFilePath)
		table = gumshoe.NewFactTable(s.Config.TableFilePath, s.Config.ToSchema())
		table.SaveToDisk()
		log.Print("done.")
	} else {
		log.Printf("loaded %d rows.", table.Count)
	}
	s.Table = table
}

func (s *Server) RunBackgroundSaves() {
	for _ = range time.Tick(s.Config.SaveDuration.Duration) {
		log.Println("Saving to disk...")
		s.Table.SaveToDisk()
		log.Println("...done saving to disk")
	}
}

// NewServer initializes a Server with a fact table from disk and sets up its routes.
func NewServer(config *config.Config) *Server {
	s := &Server{Config: config}

	m := martini.Classic()
	// Use a specific set of middlewares instead of the defaults. Note that we've removed panic recovery.
	m.Handlers(martini.Logger(), martini.Static("public"))

	// TODO(philc): Make these REST routes more consistent.
	m.Get("/metricz", s.HandleMetricz)
	m.Put("/insert", s.HandleInsertRoute)
	m.Get("/tables/facts", s.HandleFactTableRoute)
	m.Get("/tables/dimensions", s.HandleDimensionsTableRoute)
	m.Post("/tables/facts/query", s.HandleQueryRoute)

	s.Handler = m
	return s
}

func (s *Server) ListenAndServe() error {
	log.Println("Now serving on", s.Config.ListenAddr)
	server := &http.Server{
		Addr:    s.Config.ListenAddr,
		Handler: s,
	}
	return server.ListenAndServe()
}

func main() {
	flag.Parse()
	// Set configuration defaults
	config := &config.Config{
		TableFilePath: "db/table",
		SaveDuration:  config.Duration{10 * time.Second},
	}
	if _, err := toml.DecodeFile(*configFile, config); err != nil {
		log.Fatal(err)
	}
	if err := config.Validate(); err != nil {
		log.Fatal(err)
	}

	// Use all available cores for servicing requests in parallel.
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	// Set up the pprof server, if enabled.
	if *profileAddr != "" {
		go func() {
			log.Println("Pprof listening on", *profileAddr)
			log.Printf("Go to http://%s/debug/pprof to see more", *profileAddr)
			log.Fatal(http.ListenAndServe(*profileAddr, nil))
		}()
	}

	server := NewServer(config)
	server.loadFactTable()
	go server.RunBackgroundSaves()
	log.Fatal(server.ListenAndServe())
}
