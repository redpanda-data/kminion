package main

import (
	"fmt"
	"github.com/google-cloud-tools/kafka-minion/collector"
	"github.com/google-cloud-tools/kafka-minion/options"
	"github.com/kelseyhightower/envconfig"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
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

	log.Info("Starting kafka minion version%v", opts.Version)
	kafkaCollector, err := collector.NewKafkaCollector(opts)
	if err != nil {
		log.Fatal("Could not create kafka exporter. ", err)
	}
	prometheus.MustRegister(kafkaCollector)
	log.Infof("Successfully started kafka exporter")

	// Start listening on /metrics endpoint
	http.Handle("/metrics", promhttp.Handler())
	http.Handle("/healthcheck", healthcheck(kafkaCollector))
	listenAddress := fmt.Sprintf(":%d", opts.Port)
	log.Fatal(http.ListenAndServe(listenAddress, nil))
	log.Infof("Listening on: '%s", listenAddress)
}

func healthcheck(kafkaCollector *collector.Collector) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Debug("Healthcheck has been called")
		isHealthy := kafkaCollector.IsHealthy()
		if isHealthy {
			w.Write([]byte("Status: Healthy"))
		} else {
			http.Error(w, "Healthcheck failed", http.StatusServiceUnavailable)
		}
	})
}
