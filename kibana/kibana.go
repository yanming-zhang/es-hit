package kibana

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	simplejson "github.com/bitly/go-simplejson"
	"github.com/yanming-zhang/es-hit/graphite"
	log "github.com/sirupsen/logrus"

	"github.com/olivere/elastic"
)

const kibanaIndexName = ".kibana"

// Kibana for get saved search
type Kibana struct {
	Name             string `toml:"name"`
	KibanaEsURL      string `toml:"kibana_es_url"`
	KibanaEsUser     string `toml:"kibana_es_user"`
	KibanaEsPass     string `toml:"kibana_es_pass"`
	Index            string `toml:"index"`
	ShouldPrefixWith string `toml:"should_prefix_with"`
	WatchInterval    string `toml:"watch_interval"`
	CheckEvery       string `toml:"check_every"`
	SearchField      string `toml:"search_field"`
	TimeField        string `toml:"time_field"`
	Enabled          bool   `toml:"enabled"`
}

// Wrapper is main structure for query target elasticsearch
type Wrapper struct {
	Client        *elastic.Client
	Kibana        *Kibana
	CheckInterval time.Duration
	WatchInterval time.Duration
	ToSearch      chan map[string]string
	Cancel        func()
	SearchingMap  map[string]string
}

// NewWrapper create new ES instance for query kibana indes
func NewWrapper(kibana *Kibana) (*Wrapper, error) {
	client, err := elastic.NewClient(elastic.SetURL(kibana.KibanaEsURL),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
		elastic.SetBasicAuth(kibana.KibanaEsUser, kibana.KibanaEsPass))

	if err != nil {
		return nil, fmt.Errorf("failed to create ElasticSearch Client for %s, %v",
			kibana.KibanaEsURL, err)
	}

	checkInternal, err := time.ParseDuration(kibana.CheckEvery)
	if err != nil {
		log.Fatalf("Can't not parse Check Interval string %s, %v", kibana.CheckEvery, err)
	}
	watchInternal, err := time.ParseDuration(kibana.WatchInterval)
	if err != nil {
		log.Fatalf("Can't not parse Watch Interval string %s, %v", kibana.WatchInterval, err)
	}

	return &Wrapper{Client: client, Kibana: kibana,
		CheckInterval: checkInternal, WatchInterval: watchInternal,
		ToSearch: make(chan map[string]string, 1)}, nil
}

// KSearch get from kibana
type KSearch struct {
	Search SavedSearch `json:"search"`
}

// SavedSearch get from kibana
type SavedSearch struct {
	Title           string          `json:"title"`
	SavedObjectMeta SavedObjectMeta `json:"kibanaSavedObjectMeta"`
}

// SavedObjectMeta inside SavedSarch
type SavedObjectMeta struct {
	SearchSourceJSON string `json:"searchSourceJSON"`
}

// StartQueryPool for target kibana
func (w *Wrapper) StartQueryPool(ctx context.Context, kibanaWg *sync.WaitGroup, graphiteWorker *graphite.Worker) {
	// This kibana wait group
	defer kibanaWg.Done()

	go w.watchKibanaChange(ctx)

	for {
		select {
		case toSearch := <-w.ToSearch:
			log.Debug("StartQueryPool Get update from ToSearch channel.")
			if reflect.DeepEqual(toSearch, w.SearchingMap) {
				log.Debugf("Nothing change, let's move on")
				continue
			}
			if w.SearchingMap != nil {
				w.Cancel() // cancel previous running
			}
			ctx, cancel := context.WithCancel(ctx)
			w.Cancel = cancel
			w.SearchingMap = toSearch
			go func() {
				log.Infof("Run total %d queries for each saved search from %v",
					len(toSearch), w.Kibana.KibanaEsURL)
				for name, q := range w.SearchingMap {
					time.Sleep(time.Second)
					go w.runQueryLoop(ctx, name, q, graphiteWorker)
				}
			}()
		case <-ctx.Done():
			log.Infof("StartQeuryPool %s ctx done %v", w.Kibana.Name, ctx.Err())
			return
		}
	}
}

func (w *Wrapper) watchKibanaChange(ctx context.Context) {
	ticker := time.NewTicker(w.WatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			go func() {
				log.Debugf("start watching %s change", w.Kibana.Name)
				ctx, cancel := context.WithDeadline(ctx, time.Now().Add(w.WatchInterval))
				defer cancel()
				w.ToSearch <- w.getSavedSearch(ctx)
			}()
		case <-ctx.Done():
			log.Infof("watchKibanaChange %s ctx done %v", w.Kibana.Name, ctx.Err())
			return
		}
	}
}

func (w *Wrapper) getSavedSearch(ctx context.Context) map[string]string {
	client, err := elastic.NewClient(elastic.SetURL(w.Kibana.KibanaEsURL),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
		elastic.SetBasicAuth(w.Kibana.KibanaEsUser, w.Kibana.KibanaEsPass))

	if err != nil {
		log.Errorf("watchKibana Failed to Make ES client for %v, %v", w.Kibana.KibanaEsURL, err)
		return nil
	}

	log.Infof("start to query %s with index: %s", w.Kibana.KibanaEsURL, kibanaIndexName)
	searchResult, err := client.Search().
		Index(kibanaIndexName).
		From(0).
		Size(1000).
		Do(ctx)
	if err != nil {
		log.Errorf("Failed to search %s kibana indiex for geting Saved Search %v",
			kibanaIndexName, err)
		return nil
	}

	hitsCount := searchResult.TotalHits()
	log.Infof("Query Index %s took %d milliseconds found %d saved search",
		kibanaIndexName, searchResult.TookInMillis, hitsCount)

	savedSearch := filterSavedSearch(searchResult, w.Kibana.ShouldPrefixWith)
	return savedSearch
}

func (w *Wrapper) runQueryLoop(ctx context.Context, ruleName, ruleQuery string, graphiteWorker *graphite.Worker) {

	log.Infof("Run query loop for %s,  %s", ruleName, ruleQuery)
	ticker := time.NewTicker(w.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			go func() {
				ctx, cancel := context.WithDeadline(ctx, time.Now().Add(w.CheckInterval))
				defer cancel()
				log.Infof("Making search %s, against %s with raw query: %s",
					w.Kibana.KibanaEsURL, w.Kibana.Index, ruleQuery)
				hits := w.getTotalHits(ctx, ruleQuery)
				graphiteWorker.DoSend(ruleName, float64(hits))
			}()
		case <-ctx.Done():
			log.Infof("runSearch ctx done %v", ctx.Err())
			return
		}
	}
}

func (w *Wrapper) getTotalHits(ctx context.Context, rawQuery string) int64 {
	from := fmt.Sprintf("now-%s", w.Kibana.CheckEvery)
	realQ := elastic.NewBoolQuery()

	queryTime := elastic.NewRangeQuery(w.Kibana.TimeField).Gte(from).Lte("now")
	realQ = realQ.Filter(queryTime)

	newSearchTerm := fmt.Sprintf("{\"query_string\":{ \"query\": %s}  }", rawQuery)
	log.Infof("Going to Query for %s", newSearchTerm)
	// use Raw String Query for kibana saved search
	querySearch := elastic.RawStringQuery(newSearchTerm)
	realQ = realQ.Must(querySearch)

	searchResult, err := w.Client.Search().
		Index(w.Kibana.Index).
		Query(realQ).
		Do(context.Background())
	if err != nil {
		log.Errorf("Failed to Query for %s, %v", newSearchTerm, err)
		return -1
	}

	hitsCount := searchResult.TotalHits()
	log.Debugf("Query took %d milliseconds found %d log match hit",
		searchResult.TookInMillis, hitsCount)

	return hitsCount
}

func filterSavedSearch(kibanaSearch *elastic.SearchResult, prefix string) map[string]string {
	var savedRuleMap = make(map[string]string)
	log.Infof("Found %d hits from kinana search", kibanaSearch.Hits.TotalHits)
	var search KSearch
	for _, item := range kibanaSearch.Each(reflect.TypeOf(search)) {
		s := item.(KSearch)
		if strings.HasPrefix(s.Search.Title, prefix) {

			dataJSON, err := simplejson.NewJson([]byte(s.Search.SavedObjectMeta.SearchSourceJSON))
			if err != nil {
				log.Errorf("simplejson Failed to make NewJson from %v, %v",
					s.Search.SavedObjectMeta.SearchSourceJSON, err)
				continue
			}
			jsonQuery, err := dataJSON.Get("query").Get("query").Encode()
			if err != nil {
				log.Errorf("dataJSON failed %v", err)
			} else {
				jsonQueryStr := string(jsonQuery)
				log.Debugf("Matched query are: %v", jsonQueryStr)
				savedRuleMap[s.Search.Title] = jsonQueryStr
			}
		}
	}
	log.Infof("Found %d matched results from kibana", len(savedRuleMap))
	return savedRuleMap

}
