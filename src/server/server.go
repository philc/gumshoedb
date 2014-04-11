// A web server which runs the database as a daemon, and exposes routes for ingesting rows and executing
// queries.

package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"

	"config"
	"gumshoe"

	"github.com/codegangsta/martini"
)

var (
	configFile  = flag.String("config", "config.toml", "Configuration file to use")
	profileAddr = flag.String("profile-addr", "", "If non-empty, address for net/http/pprof")
)

type Server struct {
	http.Handler
	Config *config.Config
	DB     *gumshoe.DB
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
	log.Print(err)
	http.Error(w, err.Error(), status)
}

func (s *Server) Flush() {
	// NOTE(caleb): Right now there's no great way to recover from a flush error. These could occur for reasons
	// such as the disk being full or having bad permissions. For now, we'll just log and crash hard. Note that
	// the metadata is written atomically after a succesful flush, so restarting will return us to a consistent
	// state (but missing any data since the previous successful flush).
	if err := s.DB.Flush(); err != nil {
		log.Printf(">>> FATAL ERROR ON FLUSH: %s", err)
		os.Exit(1)
	}
}

// HandleInsert decodes an array of JSON-formatted row maps from the request body and inserts them into the
// database. Then it flushes before it returns.
func (s *Server) HandleInsert(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var rows []gumshoe.RowMap
	if err := decoder.Decode(&rows); err != nil {
		WriteError(w, err, http.StatusBadRequest)
		return
	}
	log.Printf("Inserting %d rows", len(rows))
	if err := s.DB.Insert(rows); err != nil {
		WriteError(w, err, http.StatusBadRequest)
		return
	}
	s.Flush()
}

// HandleDebugRows responds to the client with a JSON representation of the physical rows. It returns up to
// the first 100 rows in the database.
func (s *Server) HandleDebugRows(w http.ResponseWriter, r *http.Request) {
	WriteJSONResponse(w, s.DB.GetDebugRows())
}

// HandleDimensionTables responds with the JSON-formatted contents of all the dimension tables.
func (s *Server) HandleDimensionTables(w http.ResponseWriter, r *http.Request) {
	WriteJSONResponse(w, s.DB.GetDimensionTables())
}

// HandleSingleDimension responds with dimension table array for a single dimension.
func (s *Server) HandleSingleDimension(w http.ResponseWriter, params martini.Params) {
	name := params["name"]
	if name == "" {
		http.Error(w, "Must provide dimension name", http.StatusBadRequest)
		return
	}
	dimensionTables := s.DB.GetDimensionTables()
	if values, ok := dimensionTables[name]; ok {
		WriteJSONResponse(w, values)
		return
	}
	http.Error(w, "No such dimension: "+name, http.StatusBadRequest)
}

// HandleQuery evaluates a query and returns an aggregated result set.
// See the README for the query JSON structure and the structure of the reuslts.
func (s *Server) HandleQuery(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	query, err := s.DB.ParseJSONQuery(r.Body)
	if err != nil {
		WriteError(w, err, http.StatusBadRequest)
		return
	}
	rows, err := s.DB.GetQueryResult(query)
	if err != nil {
		WriteError(w, err, http.StatusBadRequest)
		return
	}
	elapsed := time.Since(start)
	results := map[string]interface{}{
		"results":  rows,
		"duration": elapsed.Seconds() * 1000.0,
	}
	WriteJSONResponse(w, results)
}

// HandleMetricz writes a metricz page.
func (s *Server) HandleMetricz(w http.ResponseWriter, r *http.Request) {
	metricz, err := s.makeMetricz()
	if err != nil {
		WriteError(w, err, 500)
		return
	}
	if err := metriczTemplate.Execute(w, metricz); err != nil {
		WriteError(w, err, 500)
		return
	}
}

// NewServer initializes a Server with a DB and sets up its routes.
func NewServer(conf *config.Config, schema *gumshoe.Schema) *Server {
	s := &Server{Config: conf}
	s.loadDB(schema)

	m := martini.Classic()
	// Use a specific set of middlewares instead of the defaults. Note that we've removed panic recovery.
	m.Handlers(martini.Logger(), martini.Static("public"))

	m.Put("/insert", s.HandleInsert)
	m.Get("/dimension_tables", s.HandleDimensionTables)
	m.Get("/dimension_tables/:name", s.HandleSingleDimension)
	m.Post("/query", s.HandleQuery)

	m.Get("/metricz", s.HandleMetricz)
	m.Get("/debug/rows", s.HandleDebugRows)

	s.Handler = m

	go s.RunPeriodicFlushes()
	return s
}

// loadDB opens the database if it exists, or else creates a new one.
func (s *Server) loadDB(schema *gumshoe.Schema) {
	dir := s.Config.DatabaseDir
	log.Printf(`Trying to load %q...`, dir)
	db, err := gumshoe.OpenDB(schema)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatal(err)
		}
		log.Printf(`Database %q does not exist; creating`, dir)
		db, err = gumshoe.NewDB(schema)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Database at %q created successfully", dir)
	} else {
		log.Printf("Loaded database with %d rows", db.GetNumRows())
	}
	s.DB = db
}

func (s *Server) RunPeriodicFlushes() {
	for _ = range time.Tick(s.Config.FlushInterval.Duration) {
		log.Print("Flushing to disk...")
		s.Flush()
		log.Print("Done flushing")
	}
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

	f, err := os.Open(*configFile)
	if err != nil {
		log.Fatal(err)
	}
	conf, schema, err := config.LoadTOMLConfig(f)
	if err != nil {
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

	server := NewServer(conf, schema)
	log.Fatal(server.ListenAndServe())
}
