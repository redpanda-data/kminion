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
	startCollector(opts)

	// Start listening on /metrics endpoint
	http.Handle("/metrics", promhttp.Handler())
	listenAddress := fmt.Sprintf(":%d", opts.Port)
	log.Fatal(http.ListenAndServe(listenAddress, nil))
	log.Infof("Listening on: '%s", listenAddress)

}

func startCollector(opts *options.Options) {
	log.Infof("Starting kafka lag exporter v%v", opts.Version)

	// Start kafka exporter
	log.Infof("Starting kafka exporter")
	kafkaCollector, err := collector.NewKafkaCollector(opts)
	if err != nil {
		log.Fatal("Could not create kafka exporter. ", err)
	}
	prometheus.MustRegister(kafkaCollector)
	log.Infof("Successfully started kafka exporter")
}
