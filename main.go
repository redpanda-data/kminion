package main

import (
	"github.com/google-cloud-tools/kafka-minion/collector"
	"github.com/google-cloud-tools/kafka-minion/kafka"
	"github.com/google-cloud-tools/kafka-minion/options"
	"github.com/google-cloud-tools/kafka-minion/storage"
	"github.com/kelseyhightower/envconfig"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"net"
	"net/http"
	"os"
	"strconv"
)

func main() {
	// Initialize logger
	log.SetOutput(os.Stdout)
	log.SetFormatter(&log.JSONFormatter{})

	// Parse and validate environment variables
	opts := options.NewOptions()
	var err error
	err = envconfig.Process("", opts)
	if err != nil {
		log.Fatal("Error parsing env vars into opts. ", err)
	}

	// Set log level from environment variables
	level, err := log.ParseLevel(opts.LogLevel)
	if err != nil {
		log.Panicf("Loglevel could not be parsed. See logrus documentation for valid log level inputs. Given input was '%v'", opts.LogLevel)
	}
	log.SetLevel(level)

	log.Infof("Starting kafka minion version%v", opts.Version)
	// Create cross package shared dependencies
	consumerOffsetsCh := make(chan *kafka.StorageRequest, 1000)
	clusterCh := make(chan *kafka.StorageRequest, 200)

	// Create storage module
	cache := storage.NewMemoryStorage(consumerOffsetsCh, clusterCh)
	cache.Start()

	// Create cluster module
	cluster := kafka.NewCluster(opts, clusterCh)
	cluster.Start()

	// Create kafka consumer
	consumer := kafka.NewOffsetConsumer(opts, consumerOffsetsCh)
	consumer.Start()

	// Create prometheus collector
	collector := collector.NewCollector(opts, cache)
	prometheus.MustRegister(collector)

	// Start listening on /metrics endpoint
	http.Handle("/metrics", promhttp.Handler())
	http.Handle("/healthcheck", healthCheck(cluster))
	http.Handle("/readycheck", readyCheck(cache))
	listenAddress := net.JoinHostPort(opts.TelemetryHost, strconv.Itoa(opts.TelemetryPort))
	log.Infof("Listening on: '%s", listenAddress)
	log.Fatal(http.ListenAndServe(listenAddress, nil))
}

func healthCheck(cluster *kafka.Cluster) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cluster.IsHealthy() {
			w.Write([]byte("Healthy"))
		} else {
			http.Error(w, "Healthcheck failed", http.StatusServiceUnavailable)
		}
	})
}

// readyCheck only returns 200 when it has initially consumed the __consumer_offsets topic
// Utilizing this ready check you can ensure to slow down rolling updates until a pod is ready
// to expose consumer group metrics which are up to date
func readyCheck(storage *storage.MemoryStorage) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if storage.IsConsumed() {
			w.Write([]byte("Ready"))
		} else {
			http.Error(w, "Offsets topic has not been consumed yet", http.StatusServiceUnavailable)
		}
	})
}
