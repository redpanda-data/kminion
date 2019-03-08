package main

import (
	"fmt"
	"github.com/google-cloud-tools/kafka-minion/kafka"
	"github.com/google-cloud-tools/kafka-minion/options"
	"github.com/kelseyhightower/envconfig"
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
	consumer, err := kafka.NewOffsetConsumer(opts)
	if err != nil {
		log.Panicf("Could not create offset consumer: %v", err)
	}
	log.Infof("Successfully started kafka exporter")
	consumer.Start()

	// Start listening on /metrics endpoint
	listenAddress := fmt.Sprintf(":%d", opts.Port)
	log.Fatal(http.ListenAndServe(listenAddress, nil))
	log.Infof("Listening on: '%s", listenAddress)
}

func healthcheck() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Status: Healthy"))
	})
}
