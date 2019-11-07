package graphite

import (
	"fmt"
	"net"

	log "github.com/sirupsen/logrus"
	"github.com/yanming-zhang/graphite-golang"
)

// Config for Graphite
type Config struct {
	Host   string `toml:"host"`
	Port   int    `toml:"port"`
	Prefix string `toml:"prefix"`
}

// Worker for sending metric
type Worker struct {
	config    Config
	graphite  *graphite.Graphite
	IsStarted bool
}

// NewWorker create a new worker
func NewWorker(config Config) *Worker {
	log.Infof("NewWorker Loaded Graphite connection: %#v", config)
	newGraphite, err := graphite.NewGraphite(config.Host, config.Port)
	if err != nil {
		log.Fatalf("Failed to make graphite instance: %v", err)
	}

	return &Worker{config: config, graphite: newGraphite, IsStarted: true}
}

// DoSend the metrics
func (w *Worker) DoSend(path string, value float64) {
	key := fmt.Sprintf("%s.%s.count", w.config.Prefix, path)
	log.Debugf("Graphite do send : %s with value: %s", key, fmt.Sprint(value))
	if err := w.graphite.SimpleSend(key, fmt.Sprint(value)); err != nil {
		log.Warnf("Failed to do Graphite send : %v", err)
	}
}

func (w Worker) GetConn() net.Conn {
	return w.graphite.GetNetconn()
}
