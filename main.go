package main

import (
	"context"
	"fmt"
	"github.com/cloudhut/kminion/v2/kafka"
	"github.com/cloudhut/kminion/v2/minion"
	"github.com/cloudhut/kminion/v2/prometheus"
	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"net"
	"net/http"
	"os"
	"time"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(fmt.Errorf("failed to create startup logger: %w", err))
	}

	cfg, err := newConfig(logger)
	if err != nil {
		logger.Fatal("failed to parse config", zap.Error(err))
	}
	logger.Info("started kminion, starting setup")

	kafkaSvc, err := kafka.NewService(cfg.Kafka, logger)
	if err != nil {
		logger.Fatal("failed to setup kafka service", zap.Error(err))
	}
	connectCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err = kafkaSvc.TestConnection(connectCtx)
	if err != nil {
		logger.Fatal("failed to test connectivity to Kafka cluster", zap.Error(err))
	}

	minionSvc, err := minion.NewService(cfg.Minion, logger, kafkaSvc)
	if err != nil {
		logger.Fatal("failed to setup minion service", zap.Error(err))
	}
	// TODO: Use context that cancels upon sigkill/sigterm
	err = minionSvc.Start(context.Background())
	if err != nil {
		logger.Fatal("failed to start minion service", zap.Error(err))
	}

	exporter, err := prometheus.NewExporter(cfg.Exporter, logger, minionSvc)
	if err != nil {
		logger.Fatal("failed to setup prometheus exporter", zap.Error(err))
	}
	exporter.InitializeMetrics()
	promclient.MustRegister(exporter)
	http.Handle("/metrics",
		promhttp.InstrumentMetricHandler(
			promclient.DefaultRegisterer,
			promhttp.HandlerFor(
				promclient.DefaultGatherer,
				promhttp.HandlerOpts{},
			),
		),
	)

	address := net.JoinHostPort("", "8080")
	logger.Info("listening on address", zap.String("listen_address", address))
	if err := http.ListenAndServe(address, nil); err != nil {
		logger.Error("error starting HTTP server", zap.Error(err))
		os.Exit(1)
	}
}
