package webinflux

import (
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocraft/web"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
)

type Item struct {
	Name        string
	Requests    uint64
	StatusCodes map[int]uint64
	Timer       metrics.Timer
}

type item struct {
	name           string
	requests       uint64
	statusCodes    map[int]*uint64
	timer          metrics.Timer
	statusCodeLock sync.RWMutex
}

func (i *item) snapshot() Item {
	newItem := Item{
		Name:        i.name,
		StatusCodes: make(map[int]uint64),
		Timer:       i.timer.Snapshot(),
	}
	newItem.Requests = atomic.SwapUint64(&i.requests, 0)
	i.statusCodeLock.RLock()
	for sc, v := range i.statusCodes {
		newItem.StatusCodes[sc] = atomic.SwapUint64(v, 0)
	}
	i.statusCodeLock.RUnlock()
	return newItem
}

func (i *item) addRequest(code int, took time.Duration) {
	var ok bool
	var valuePtr *uint64

	atomic.AddUint64(&i.requests, 1)
	i.timer.Update(took)

	i.statusCodeLock.RLock()
	valuePtr, ok = i.statusCodes[code]
	if ok {
		i.statusCodeLock.RUnlock()
		atomic.AddUint64(valuePtr, 1)
		return
	}
	i.statusCodeLock.RUnlock()
	i.statusCodeLock.Lock()
	valuePtr, ok = i.statusCodes[code]
	if ok {
		i.statusCodeLock.Unlock()
		atomic.AddUint64(valuePtr, 1)
	}
	var value uint64 = 1
	i.statusCodes[code] = &value
	i.statusCodeLock.Unlock()

}

type Middleware struct {
	items     map[string]*item
	itemsLock sync.RWMutex
	interval  time.Duration

	influxdb          client.Client
	name              string
	influxdb_url      string
	influxdb_database string
	influxdb_username string
	influxdb_password string
	influxdb_tags     map[string]string
}

func (m *Middleware) send() error {
	items := m.Snapshot()

	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  m.influxdb_database,
		Precision: "s",
	})
	if err != nil {
		log.WithFields(log.Fields{
			"error":    err,
			"url":      m.influxdb_url,
			"database": m.influxdb_database,
			"username": m.influxdb_username,
		}).Error("unable to create new batch points")
		return err
	}

	now := time.Now()
	for _, item := range items {
		tags := make(map[string]string)
		for k, v := range m.influxdb_tags {
			tags[k] = v
		}
		tags["route"] = item.Name
		ps := item.Timer.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
		fields := map[string]interface{}{
			"count":    item.Timer.Count(),
			"max":      item.Timer.Max(),
			"mean":     item.Timer.Mean(),
			"min":      item.Timer.Min(),
			"stddev":   item.Timer.StdDev(),
			"variance": item.Timer.Variance(),
			"p50":      ps[0],
			"p75":      ps[1],
			"p95":      ps[2],
			"p99":      ps[3],
			"p999":     ps[4],
			"p9999":    ps[5],
			"m1":       item.Timer.Rate1(),
			"m5":       item.Timer.Rate5(),
			"m15":      item.Timer.Rate15(),
			"meanrate": item.Timer.RateMean(),
			"requests": item.Requests,
		}
		for code, count := range item.StatusCodes {
			fields[fmt.Sprintf("status.%d", code)] = count
		}
		point, err := client.NewPoint(m.name, tags, fields, now)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("unable to create new points")
			return err
		}
		bp.AddPoint(point)
	}
	err = m.influxdb.Write(bp)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("influxdb.Write failed")
	}

	return nil
}

func (m *Middleware) run() {
	var err error
	intervalTicker := time.Tick(m.interval)
	pingTicker := time.Tick(time.Second * 5)
	for {
		select {
		case <-intervalTicker:
			err = m.send()
			if err != nil {
				log.WithFields(log.Fields{
					"error":    err,
					"url":      m.influxdb_url,
					"username": m.influxdb_username,
				}).Error("unable to send metrics to InfluxDB")
			}
		case <-pingTicker:
			_, _, err = m.influxdb.Ping(time.Second)
			if err != nil {
				log.WithFields(log.Fields{
					"error":    err,
					"url":      m.influxdb_url,
					"username": m.influxdb_username,
				}).Error("error sending a ping to InfluxDB")

				err = m.connect()
				if err != nil {
					log.WithFields(log.Fields{
						"error":    err,
						"url":      m.influxdb_url,
						"username": m.influxdb_username,
					}).Error("unable to recreate InfluxDB client")
				}
			}
		}
	}
}

func (m *Middleware) connect() error {
	var err error
	m.influxdb, err = client.NewHTTPClient(client.HTTPConfig{
		Addr:     m.influxdb_url,
		Username: m.influxdb_username,
		Password: m.influxdb_password,
	})

	if err != nil {
		log.WithFields(log.Fields{
			"url":      m.influxdb_url,
			"username": m.influxdb_username,
			"error":    err,
		}).Error("unable to connect to InfluxDB")
		return err
	}

	_, _, err = m.influxdb.Ping(time.Second)
	if err != nil {
		log.WithFields(log.Fields{
			"url":      m.influxdb_url,
			"username": m.influxdb_username,
			"error":    err,
		}).Error("unable to ping InfluxDB")
		return err
	}

	return nil
}

func (m *Middleware) Snapshot() []Item {
	var items []Item
	var itemPtrs []*item

	m.itemsLock.RLock()
	for _, item := range m.items {
		itemPtrs = append(itemPtrs, item)
	}
	m.itemsLock.RUnlock()

	for _, item := range itemPtrs {
		items = append(items, item.snapshot())
	}

	return items
}

func (m *Middleware) getItem(name string) *item {
	var nameItem *item
	var ok bool
	m.itemsLock.RLock()
	nameItem, ok = m.items[name]
	if ok {
		m.itemsLock.RUnlock()
		return nameItem
	}
	m.itemsLock.RUnlock()
	m.itemsLock.Lock()
	nameItem, ok = m.items[name]
	if ok {
		m.itemsLock.Unlock()
		return nameItem
	}
	nameItem = &item{
		name:        name,
		statusCodes: make(map[int]*uint64),
		timer:       metrics.NewTimer(),
	}
	m.items[name] = nameItem
	m.itemsLock.Unlock()
	return nameItem
}

func NewWebInflux(name, influxdb_url, influxdb_database, influxdb_username, influxdb_password string, influxdb_tags map[string]string) (*Middleware, error) {
	_, err := url.Parse(influxdb_url)
	if err != nil {
		log.WithFields(log.Fields{
			"url":   influxdb_url,
			"error": err,
		}).Error("failed to parse influxdb URL")
		return nil, err
	}

	m := Middleware{
		items:             make(map[string]*item),
		interval:          time.Second * 10,
		name:              name,
		influxdb_url:      influxdb_url,
		influxdb_database: influxdb_database,
		influxdb_username: influxdb_username,
		influxdb_password: influxdb_password,
		influxdb_tags:     influxdb_tags,
	}

	err = m.connect()
	if err != nil {
		return nil, err
	}

	go m.run()

	return &m, nil
}

func (m *Middleware) ServeHTTP(rw web.ResponseWriter, req *web.Request, next web.NextMiddlewareFunc) {
	start := time.Now()
	next(rw, req)
	took := time.Since(start)

	route := req.RoutePath()

	m.getItem(route).addRequest(rw.StatusCode(), took)
}