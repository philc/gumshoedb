package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/philc/gumshoedb/gumshoe"
	"github.com/philc/gumshoedb/internal/config"

	"github.com/philc/gumshoedb/internal/github.com/pkg/sftp"
	"github.com/philc/gumshoedb/internal/golang.org/x/crypto/ssh"
	"github.com/philc/gumshoedb/internal/golang.org/x/crypto/ssh/agent"
)

func init() {
	commandsByName["balance"] = command{
		description: "balance intervals amongst a set of GumshoeDB shards",
		fn:          balance,
	}
}

func balance(args []string) {
	flags := flag.NewFlagSet("gumtool balance", flag.ExitOnError)
	var (
		username      string
		sourceDBDirs  stringsFlag
		destDBConfigs stringsFlag
		binDir        string
		workDir       string
	)
	flags.StringVar(&username, "username", "", "SSH username")
	flags.Var(&sourceDBDirs, "source-db-dirs",
		"Dirs of source DBs in form host.com:/path/to/db; comma-separated")
	flags.Var(&destDBConfigs, "dest-db-configs",
		"Filenames of dest DB configs in form host.com:/path/to/conf.toml; comma-separated")
	flags.StringVar(&binDir, "bindir", "", "Path to dir containing gumshoedb binaries (same on all servers)")
	flags.StringVar(&workDir, "workdir", "", "Path to dir for temporary partial DBs (same on all servers)")
	flags.Parse(args)

	if len(sourceDBDirs) == 0 {
		log.Fatal("Need at least one dir in -source-db-dirs")
	}
	if len(destDBConfigs) == 0 {
		log.Fatal("Need at least one config in -dest-db-configs")
	}
	if binDir == "" {
		log.Fatal("-bindir must be given")
	}
	if workDir == "" {
		log.Fatal("-workdir must be given")
	}

	sources := make([]balanceSource, len(sourceDBDirs))
	for i, hostDir := range sourceDBDirs {
		parts := strings.SplitN(hostDir, ":", 2)
		if len(parts) != 2 {
			log.Fatalln("Bad host:dir:", hostDir)
		}
		sources[i] = balanceSource{parts[0], parts[1]}
	}

	dests := make([]balanceDest, len(destDBConfigs))
	for i, hostConfig := range destDBConfigs {
		parts := strings.SplitN(hostConfig, ":", 2)
		if len(parts) != 2 {
			log.Fatalln("Bad host:config:", hostConfig)
		}
		dests[i] = balanceDest{parts[0], parts[1]}
	}

	s := NewSSH(username)

	//
	// 1. Load all the schemas from the source DB dirs and check mutual equivalence.
	//

	// These *gumshoeDBs only have schemas and static table metadata filled out.
	sourceDBs := make([]*gumshoe.DB, len(sources))
	for i, source := range sources {
		sftpClient, err := s.getSFTPClient(source.host)
		if err != nil {
			log.Fatal(err)
		}
		dbJSONPath := path.Join(source.dbDir, "db.json")
		f, err := sftpClient.Open(dbJSONPath)
		if err != nil {
			log.Fatalf("Cannot open %s:%s: %s", source.host, dbJSONPath, err)
		}
		db := new(gumshoe.DB)
		if err := json.NewDecoder(f).Decode(db); err != nil {
			log.Fatalf("Error loading db.json schema from %s: %s", source.host, err)
		}
		f.Close()
		sourceDBs[i] = db
		if i > 0 {
			if err := sourceDBs[0].Schema.Equivalent(db.Schema); err != nil {
				log.Fatalln("Found non-equivalent schemas amongst source DBs:", err)
			}
		}
	}
	log.Printf("Loaded %d source DBs and confirmed equivalence", len(sourceDBs))

	//
	// 2. Load all the dest DB configs and check equivalence.
	//

	for _, dest := range dests {
		sftpClient, err := s.getSFTPClient(dest.host)
		if err != nil {
			log.Fatal(err)
		}
		f, err := sftpClient.Open(dest.config)
		if err != nil {
			log.Fatalf("Error opening toml config at %s:%s: %s", dest.host, dest.config, err)
		}
		_, schema, err := config.LoadTOMLConfig(f)
		if err != nil {
			log.Fatalf("Error loading config TOML from %s: %s", dest.host, err)
		}
		f.Close()
		if err := sourceDBs[0].Schema.Equivalent(schema); err != nil {
			log.Fatalf("Found schema in DB config on %s that is not equivalent to the source DBs: %s", dest.host, err)
		}
	}
	log.Printf("Checked %d dest DB configs and confirmed equivalence to source DBs", len(dests))

	//
	// 3. Do the per-source work (in parallel). For each source DB:
	//    a. Partition the DB intervals by dest DB
	//    b. Create new DB dirs called /tmp/gumshoedb/db.partial.shardN
	//    c. Copy in the segment files for the interval to the appropriate partial DB
	//    d. Copy in the source dimension files to each partial DB
	//    e. Write out a db.json for each partial DB
	//    f. Gzip each partial DB
	//    g. SCP each partial DB to the dest DB host at /tmp/gumshoedb/db.partial.fromhostXXX.gz
	//

	if err := MakeWorkDirs(sources, dests, workDir, s); err != nil {
		log.Fatal(err)
	}
	var (
		wg sync.WaitGroup
		mu sync.Mutex // protects partialsByDest
		// Map each dest to all the sources that have a partial for it.
		partialsByDest = make(map[balanceDest][]balanceSource)
	)
	for i := range sourceDBs {
		source := sources[i]
		db := sourceDBs[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			partialDests, err := PreparePartials(source, dests, db, workDir, s)
			if err != nil {
				log.Fatal(err)
			}
			mu.Lock()
			for _, dest := range partialDests {
				partialsByDest[dest] = append(partialsByDest[dest], source)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	//
	// 4. Do the per-dest work (in parallel). For each dest DB:
	//    a. Un-gzip each partial DB
	//    b. gumtool merge the partial DBs
	//

	for i := range dests {
		dest := dests[i]
		sources := partialsByDest[dest]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := MergeDBs(dest, sources, workDir, binDir, s); err != nil {
				log.Fatal(err)
			}
		}()
	}
	wg.Wait()
	if err := RemoveWorkDirs(sources, dests, workDir, s); err != nil {
		log.Fatal(err)
	}
}

func PreparePartials(source balanceSource, dests []balanceDest, db *gumshoe.DB, workDir string, s *SSH) ([]balanceDest, error) {
	partials := MakePartialDBs(db, dests)
	var partialDests []balanceDest
	for i, partial := range partials {
		if partial == nil {
			continue
		}
		partialDests = append(partialDests, dests[i])
		log.Printf("Preparing partial DB for source %s, dest %s (%d intervals)",
			source.host, dests[i].host, len(partial.StaticTable.Intervals))
		partialName := PartialName(source, dests[i])
		dir := path.Join(workDir, partialName)
		if err := s.runCmd(source.host, "mkdir "+dir); err != nil {
			return nil, err
		}
		for _, interval := range partial.StaticTable.Intervals {
			for i := 0; i < interval.NumSegments; i++ {
				segmentFile := path.Join(source.dbDir, interval.SegmentFilename(db.Schema, i))
				if err := s.runCmd(source.host, fmt.Sprintf("cp %s %s", segmentFile, dir)); err != nil {
					return nil, err
				}
			}
		}
		for i, dimTable := range partial.StaticTable.DimensionTables {
			if dimTable == nil || dimTable.Size == 0 {
				continue
			}
			dimTableFile := path.Join(source.dbDir, dimTable.Filename(db.Schema, i))
			if err := s.runCmd(source.host, fmt.Sprintf("cp %s %s", dimTableFile, dir)); err != nil {
				return nil, err
			}
		}
		if err := WriteDBJSONToHost(partial, source.host, dir, s); err != nil {
			return nil, err
		}
		if source.host == dests[i].host {
			// The partial is already in place for this host.
			continue
		}
		if err := s.runCmd(source.host, fmt.Sprintf("tar -C %s -czf %s %s", workDir, dir+".tgz", path.Base(dir))); err != nil {
			return nil, err
		}
		targetPath := path.Join(workDir, partialName+".tgz")
		session, err := s.getSession(source.host)
		if err != nil {
			return nil, err
		}
		scpCmd := fmt.Sprintf("scp -o StrictHostKeyChecking=no %s %s:%s", dir+".tgz", dests[i].host, targetPath)
		if err := session.Run(scpCmd); err != nil {
			return nil, err
		}
		session.Close()
	}

	return partialDests, nil
}

func WriteDBJSONToHost(db *gumshoe.DB, host, dir string, s *SSH) error {
	sftpClient, err := s.getSFTPClient(host)
	if err != nil {
		return err
	}
	f, err := sftpClient.Create(path.Join(dir, "db.json"))
	if err != nil {
		return err
	}
	defer f.Close()
	text, err := json.MarshalIndent(db, "", "  ")
	if err != nil {
		return err
	}
	if _, err := f.Write(text); err != nil {
		return err
	}
	return f.Close()
}

func MergeDBs(dest balanceDest, sources []balanceSource, workDir, binDir string, s *SSH) error {
	var dbPaths []string
	for _, source := range sources {
		dir := path.Join(workDir, PartialName(source, dest))
		dbPaths = append(dbPaths, dir)
		if source.host == dest.host {
			continue
		}
		if err := s.runCmd(dest.host, fmt.Sprintf("tar -C %s -xzf %s", workDir, dir+".tgz")); err != nil {
			return err
		}
	}
	gumtool := path.Join(binDir, "gumtool")
	mergeCmd := fmt.Sprintf("%s merge -db-paths '%s' -new-db-config %s -parallelism 8",
		gumtool, strings.Join(dbPaths, ","), dest.config)
	log.Println("Running merge on", dest.host)
	return s.runCmd(dest.host, mergeCmd)
}

func PartialName(source balanceSource, dest balanceDest) string {
	return fmt.Sprintf("db.partial.source=%s.dest=%s", source.host, dest.host)
}

// MakePartialDBs partitions the intervals to synthesize multiple DBs spread amongst dests.
func MakePartialDBs(db *gumshoe.DB, dests []balanceDest) []*gumshoe.DB {
	partials := make([]*gumshoe.DB, len(dests))
	for i := range partials {
		partials[i] = &gumshoe.DB{
			Schema: db.Schema,
			StaticTable: &gumshoe.StaticTable{
				Intervals:       make(gumshoe.IntervalMap),
				DimensionTables: db.StaticTable.DimensionTables,
			},
		}
	}
	for t, interval := range db.StaticTable.Intervals {
		// Redistribute by spreading the intervals around by time bucket.
		intervalIdx := int(time.Duration(t.UnixNano()) / db.Schema.IntervalDuration)
		db := partials[intervalIdx%len(dests)]
		db.StaticTable.Intervals[t] = interval
	}
	// Nil out all the DBs with no data.
	for i, partial := range partials {
		if len(partial.StaticTable.Intervals) == 0 {
			partials[i] = nil
		}
	}
	return partials
}

func MakeWorkDirs(sources []balanceSource, dests []balanceDest, workDir string, s *SSH) error {
	log.Printf("Creating workdir (%s) on all hosts", workDir)
	for _, host := range uniqueHosts(sources, dests) {
		if err := s.runCmd(host, "mkdir "+workDir); err != nil {
			return err
		}
	}
	return nil
}

func RemoveWorkDirs(sources []balanceSource, dests []balanceDest, workDir string, s *SSH) error {
	log.Printf("Removing workdir (%s) on all hosts", workDir)
	for _, host := range uniqueHosts(sources, dests) {
		if err := s.runCmd(host, "rm -rf "+workDir); err != nil {
			return err
		}
	}
	return nil
}

func uniqueHosts(sources []balanceSource, dests []balanceDest) []string {
	found := make(map[string]struct{})
	var hosts []string
	for _, source := range sources {
		if _, ok := found[source.host]; !ok {
			found[source.host] = struct{}{}
			hosts = append(hosts, source.host)
		}
	}
	for _, dest := range dests {
		if _, ok := found[dest.host]; !ok {
			found[dest.host] = struct{}{}
			hosts = append(hosts, dest.host)
		}
	}
	return hosts
}

type balanceSource struct {
	host  string
	dbDir string
}

type balanceDest struct {
	host   string
	config string
}

type SSH struct {
	config          *ssh.ClientConfig
	clientCache     map[string]*ssh.Client
	sftpClientCache map[string]*sftp.Client
	agent           agent.Agent
}

func NewSSH(username string) *SSH {
	conn, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK"))
	if err != nil {
		log.Println("Cannot connect to ssh agent:", err)
		log.Fatalln("(Is your ssh-agent running? Is $SSH_AUTH_SOCK set?)")
	}
	ag := agent.NewClient(conn)
	config := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{ssh.PublicKeysCallback(ag.Signers)},
	}
	return &SSH{
		config:          config,
		clientCache:     make(map[string]*ssh.Client),
		sftpClientCache: make(map[string]*sftp.Client),
		agent:           ag,
	}
}

func (s *SSH) runCmd(host, cmd string) error {
	session, err := s.getSession(host)
	if err != nil {
		return err
	}
	defer session.Close()
	return session.Run(cmd)
}

func (s *SSH) getClient(host string) (*ssh.Client, error) {
	client, ok := s.clientCache[host]
	if !ok {
		var err error
		client, err = ssh.Dial("tcp", host+":22", s.config)
		if err != nil {
			return nil, err
		}
		s.clientCache[host] = client
		if err := agent.ForwardToAgent(client, s.agent); err != nil {
			return nil, err
		}
		// NOTE(mikeq): We can only enable forwarding once per client (for reasons we don't understand). We
		// open an initial session here and enable forwarding so that it is enabled for all sessions going
		// forward on this host.
		session, err := client.NewSession()
		if err != nil {
			return nil, err
		}
		if err := agent.RequestAgentForwarding(session); err != nil {
			return nil, err
		}
		session.Close()
	}
	return client, nil
}

func (s *SSH) getSession(host string) (*ssh.Session, error) {
	client, err := s.getClient(host)
	if err != nil {
		return nil, err
	}
	session, err := client.NewSession()
	if err != nil {
		return nil, err
	}
	session.Stdout = stdoutPrinter(host)
	session.Stderr = stderrPrinter(host)
	return session, nil
}

func (s *SSH) getSFTPClient(host string) (*sftp.Client, error) {
	sftpClient, ok := s.sftpClientCache[host]
	if !ok {
		client, err := s.getClient(host)
		if err != nil {
			return nil, err
		}
		sftpClient, err = sftp.NewClient(client)
		if err != nil {
			return nil, err
		}
		s.sftpClientCache[host] = sftpClient
	}
	return sftpClient, nil
}

type Printer struct {
	Prefix1, Prefix2 string
}

var printMu sync.Mutex

func (p *Printer) Write(b []byte) (int, error) {
	printMu.Lock()
	defer printMu.Unlock()
	for _, line := range bytes.Split(b, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		if _, err := fmt.Printf("[%s|%s] %s\n", p.Prefix1, p.Prefix2, line); err != nil {
			return 0, err
		}
	}
	return len(b), nil
}

func stdoutPrinter(host string) io.Writer { return &Printer{host, "out"} }
func stderrPrinter(host string) io.Writer { return &Printer{host, "err"} }
