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
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"config"
	"gumshoe"

	"github.com/cespare/gostc"
	"github.com/cespare/hutil/apachelog"
	"github.com/gorilla/pat"
)

// No date/time in the log because it's assumed our external log handling (svlogd) takes care of that.
const logFlags = log.Lshortfile

var (
	// Flags
	configFile  = flag.String("config", "config.toml", "Configuration file to use")
	profileAddr = flag.String("profile-addr", "", "If non-empty, address for net/http/pprof")

	Log    = log.New(os.Stderr, "[server] ", logFlags)
	statsd *gostc.Client

	// Anything that needs to know about program shutdown can listen on this chan.
	shutdown = make(chan struct{})
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
	Log.Print(err)
	http.Error(w, err.Error(), status)
}

func (s *Server) Flush() {
	// NOTE(caleb): Right now there's no great way to recover from a flush error. These could occur for reasons
	// such as the disk being full or having bad permissions. For now, we'll just log and crash hard. Note that
	// the metadata is written atomically after a succesful flush, so restarting will return us to a consistent
	// state (but missing any data since the previous successful flush).
	start := time.Now()
	if err := s.DB.Flush(); err != nil {
		Log.Printf(">>> FATAL ERROR ON FLUSH: %s", err)
		os.Exit(1)
	}
	statsd.Time("gumshoedb.flush", time.Since(start))
}

// HandleInsert decodes an array of JSON-formatted row maps from the request body and inserts them into the
// database.
func (s *Server) HandleInsert(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var rows []gumshoe.RowMap
	if err := decoder.Decode(&rows); err != nil {
		WriteError(w, err, http.StatusBadRequest)
		return
	}
	Log.Printf("Inserting %d rows", len(rows))

	var success, failure float64
	if err := s.DB.Insert(rows); err == nil {
		success = float64(len(rows))
	} else {
		WriteError(w, err, http.StatusBadRequest)
		failure = float64(len(rows))
	}
	statsd.Count("gumshoedb.insert.success", success, 1)
	statsd.Count("gumshoedb.insert.failure", failure, 1)
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
func (s *Server) HandleSingleDimension(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get(":name")
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
	query, err := gumshoe.ParseJSONQuery(r.Body)
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
	statsd.Time("gumshoedb.query", elapsed)
	results := map[string]interface{}{
		"results":     rows,
		"duration_ms": int(elapsed.Seconds() * 1000),
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

func (s *Server) HandleRoot(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Gumshoe is on the case!"))
}

type Statusz struct {
	// Unix times, nullable
	LastUpdated    *int64
	OldestInterval *int64
}

func (s *Server) HandleStatusz(w http.ResponseWriter, r *http.Request) {
	var statusz Statusz
	latestTimestamp := s.DB.GetLatestTimestamp()
	lastUpdated := latestTimestamp.Unix()
	if !latestTimestamp.IsZero() {
		statusz.LastUpdated = &lastUpdated
	}
	oldestInterval := s.DB.GetOldestIntervalTimestamp()
	oldest := oldestInterval.Unix()
	if !oldestInterval.IsZero() {
		statusz.OldestInterval = &oldest
	}
	WriteJSONResponse(w, statusz)
}

// NewServer initializes a Server with a DB and sets up its routes.
func NewServer(conf *config.Config, schema *gumshoe.Schema) *Server {
	s := &Server{Config: conf}
	s.loadDB(schema)

	mux := pat.New()

	mux.Put("/insert", s.HandleInsert)
	mux.Get("/dimension_tables/{name}", s.HandleSingleDimension)
	mux.Get("/dimension_tables", s.HandleDimensionTables)
	mux.Post("/query", s.HandleQuery)

	mux.Get("/metricz", s.HandleMetricz)
	mux.Get("/debug/rows", s.HandleDebugRows)
	mux.Get("/statusz", s.HandleStatusz)
	mux.Get("/", s.HandleRoot)

	s.Handler = apachelog.NewDefaultHandler(mux)

	go s.RunPeriodicFlushes()
	go s.RunPeriodicStatsChecks()
	return s
}

// loadDB opens the database if it exists, or else creates a new one.
func (s *Server) loadDB(schema *gumshoe.Schema) {
	dir := s.Config.DatabaseDir
	Log.Printf(`Trying to load %q...`, dir)
	db, err := gumshoe.OpenDB(schema)
	if err != nil {
		if err != gumshoe.DBDoesNotExistErr {
			Log.Fatal(err)
		}
		Log.Printf(`Database %q does not exist; creating`, dir)
		db, err = gumshoe.NewDB(schema)
		if err != nil {
			Log.Fatal(err)
		}
		Log.Printf("Database at %q created successfully", dir)
	} else {
		stats := db.GetDebugStats()
		Log.Printf("Loaded database with %d rows", stats.Rows)
	}
	s.DB = db
}

func (s *Server) RunPeriodicFlushes() {
	timer := time.NewTimer(s.Config.FlushInterval.Duration)
	for {
		select {
		case <-timer.C:
			s.Flush()
			timer.Reset(s.Config.FlushInterval.Duration)
		case <-shutdown:
			s.Flush()
			os.Exit(0)
		}
	}
}

func (s *Server) RunPeriodicStatsChecks() {
	// NOTE(caleb): For now, hardcode the interval. We can adjust it or make it a configuration option later.
	for _ = range time.Tick(time.Minute) {
		stats := s.DB.GetDebugStats()
		statsd.Gauge("gumshoedb.static-table.intervals", float64(stats.Intervals))
		statsd.Gauge("gumshoedb.static-table.segments", float64(stats.Segments))
		statsd.Gauge("gumshoedb.static-table.rows", float64(stats.Rows))
		statsd.Gauge("gumshoedb.static-table.bytes", float64(stats.Bytes))
		statsd.Gauge("gumshoedb.static-table.compression-ratio", stats.CompressionRatio)
	}
}

func (s *Server) ListenAndServe() error {
	Log.Println("Now serving on", s.Config.ListenAddr)
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
		Log.Fatal(err)
	}
	defer f.Close()
	conf, schema, err := config.LoadTOMLConfig(f)
	if err != nil {
		Log.Fatal(err)
	}

	// Use all available cores for servicing requests in parallel.
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	// Try to set the RLIMIT_NOFILE to the config value. This might fail if the binary lacks sufficient
	// permissions/capabilities, or on non-Linux OSes.
	rlimit := &syscall.Rlimit{uint64(conf.OpenFileLimit), uint64(conf.OpenFileLimit)}
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, rlimit); err != nil {
		Log.Println("Error raising RLIMIT_NOFILE:", err)
	}

	// Set up the pprof server, if enabled.
	if *profileAddr != "" {
		go func() {
			Log.Println("Pprof listening on", *profileAddr)
			Log.Printf("Go to http://%s/debug/pprof to see more", *profileAddr)
			Log.Fatal(http.ListenAndServe(*profileAddr, nil))
		}()
	}

	// Configure the gumshoe logger
	gumshoe.Log = log.New(os.Stdout, "[gumshoe] ", logFlags)

	// Configure the statsd client
	statsd, err = gostc.NewClient(conf.StatsdAddr)
	if err != nil {
		Log.Fatal(err)
	}

	// Listen for signals so we can try to flush before shutdown
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		close(shutdown)
	}()

	server := NewServer(conf, schema)
	Log.Fatal(server.ListenAndServe())
}
