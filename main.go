package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	log "github.com/sirupsen/logrus"
	"github.com/yanming-zhang/es-hit/graphite"
	"github.com/yanming-zhang/es-hit/kibana"
	"github.com/yanming-zhang/es-hit/staticrule"
)

type config struct {
	Graphite    graphite.Config   `toml:"graphite"`
	StaticRules []staticrule.Rule `toml:"static-rules"`
	Kibanas     []kibana.Kibana   `toml:"kibanas"`
}

var configFile string
var verbose bool

func init() {
	// log.SetFormatter(&log.JSONFormatter{})
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)

	flag.StringVar(&configFile, "config", "", "Configuration file")
	flag.BoolVar(&verbose, "verbose", false, "Verbose output")
	flag.Parse()
}

var (
	conf           config
	graphiteWorker *graphite.Worker
	workerCh       = make(chan *graphite.Worker)

	currConn  net.Conn
	isStarted bool
)

func main() {
	if verbose {
		log.SetLevel(log.DebugLevel)
	}

	if configFile == "" {
		log.Fatalln("Please indicate config file with -config")
	}

	_, err := toml.DecodeFile(configFile, &conf)
	if err != nil {
		log.Fatalf("Failed to decode the TOML configuration file: %v, %v", configFile, err)
	}
	log.Debugln(conf)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	go graphite.NewWorker(conf.Graphite, workerCh)
	graphiteWorker, _ = <-workerCh

	ctx, cancel := context.WithCancel(context.Background())

	// Waiting for Program Inerrupt
	go func() {
		select {
		case <-signalCh:
			cancel()
		case <-graphiteWorker.StatusCh:
			currConn.Close()
			currConn = nil
			go graphite.NewWorker(conf.Graphite, workerCh)
			graphiteWorker, _ = <-workerCh
			currConn = graphiteWorker.GetConn()
			isStarted = graphiteWorker.IsStarted
		case <-ctx.Done():
			log.Infof("Parent ctx done %v", ctx.Err())
		}
	}()

	currConn = graphiteWorker.GetConn()
	isStarted = graphiteWorker.IsStarted
	go healthCheck(currConn, isStarted)

	var mainWg sync.WaitGroup
	mainWg.Add(2)
	go staticWorker(ctx, &mainWg, graphiteWorker)
	go kibanaWorker(ctx, &mainWg, graphiteWorker)
	mainWg.Wait()
}

// connection health check
func healthCheck(conn net.Conn, isStarted bool) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("The healthcheck panic and restore, %v", err)
		}
	}()

	for {
		if !isStarted {
			continue
		}

		if conn != nil {
			fmt.Fprintf(conn, "health_check\n")
			_, err := bufio.NewReader(conn).ReadString('\n')
			if err != nil {
				conn.Close()
				conn = nil
				log.Errorf("The current connection is dead, %v", err)
				goto Loop
			}
			time.Sleep(15 * time.Second)
			continue
		}
		log.Error("The current connection socket is null")

	Loop:
		graphite.NewWorker(conf.Graphite, workerCh)
		graphiteWorker, _ = <-workerCh
		currConn = graphiteWorker.GetConn()
		isStarted = graphiteWorker.IsStarted
		time.Sleep(15 * time.Second)
	}
}

// Search for static rules
func staticWorker(ctx context.Context, mainWg *sync.WaitGroup, graphiteWorker *graphite.Worker) {
	defer mainWg.Done()
	var staticWg sync.WaitGroup
	for _, rule := range conf.StaticRules {
		if !rule.Enabled {
			continue
		}
		staticWg.Add(1)
		log.Infof("Static Rule %v starting", rule.Name)
		time.Sleep(time.Second)
		go runStaticQueryLoop(ctx, &staticWg, rule, graphiteWorker)
	}

	staticWg.Wait()
	log.Warnf("Static Worker stop here.")
}

// Search for Kibana rules
func kibanaWorker(ctx context.Context, mainWg *sync.WaitGroup, graphiteWorker *graphite.Worker) {
	defer mainWg.Done()
	var kibanaWg sync.WaitGroup
	for _, k := range conf.Kibanas {
		if !k.Enabled {
			continue
		}
		kibanaWg.Add(1)
		log.Infof("Kibana Worker %v starting", k.Name)
		time.Sleep(time.Second)
		// Create new Kibana Wrapper for this kibana config
		kibanaWrapper, err := kibana.NewWrapper(&k)
		if err != nil {
			log.Errorf("Failed to create Kibana Wrapper search, %v", err)
		}
		go kibanaWrapper.StartQueryPool(ctx, &kibanaWg, graphiteWorker)
	}

	kibanaWg.Wait()
	log.Warnf("Kibana Worker stop here.")
}

func runStaticQueryLoop(ctx context.Context, staticWg *sync.WaitGroup, rule staticrule.Rule, graphiteWorker *graphite.Worker) {
	defer staticWg.Done()
	log.Infof("Run Static query loop for %s, %s", rule.Name, rule.Pattern)
	// Create new ES Wrapper for this rule
	esWrapper, err := staticrule.NewWrapper(&rule)
	if err != nil {
		log.Errorf("Failed to create ES Wrapper search, %v", err)
	}

	duration, err := time.ParseDuration(rule.CheckEvery)
	if err != nil {
		log.Fatalf("Can't not parse Check Interval string %s, %v", rule.CheckEvery, err)
	}
	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			go func() {
				ctx, cancel := context.WithDeadline(ctx, time.Now().Add(duration))
				defer cancel()
				log.Infof("Making search %s, against %s with pattern %s", rule.EsURL, rule.Index, rule.Pattern)
				hits, err := esWrapper.SearchHit(ctx)
				if err != nil {
					log.Errorf("Failed to do search, %v", err)
				}
				graphiteWorker.DoSend(rule.Name, float64(hits))
			}()
		case <-ctx.Done():
			log.Infof("runSearch ctx done %v", ctx.Err())
			return
		}
	}
}
