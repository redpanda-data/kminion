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
	"os/signal"
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
	logger.Info("started kminion", zap.String("version", cfg.Version))

	// Setup context that cancels when the application receives an interrupt signal
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
		cancel()
	}()
	go func() {
		select {
		case <-c:
			logger.Info("received a signal, going to shut down KMinion")
			cancel()
		case <-ctx.Done():
		}
	}()

	// Create kafka service and check if client can successfully connect to Kafka cluster
	kafkaSvc, err := kafka.NewService(cfg.Kafka, logger)
	if err != nil {
		logger.Fatal("failed to setup kafka service", zap.Error(err))
	}
	connectCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	err = kafkaSvc.TestConnection(connectCtx)
	if err != nil {
		logger.Fatal("failed to test connectivity to Kafka cluster", zap.Error(err))
	}

	// Create minion service that does most of the work. The Prometheus exporter only talks to the minion service
	// which issues all the requests to Kafka and wraps the interface accordingly.
	minionSvc, err := minion.NewService(cfg.Minion, logger, kafkaSvc)
	if err != nil {
		logger.Fatal("failed to setup minion service", zap.Error(err))
	}
	err = minionSvc.Start(ctx)
	if err != nil {
		logger.Fatal("failed to start minion service", zap.Error(err))
	}

	// The Prometheus exporter that implements the Prometheus collector interface
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

	// Start HTTP server
	address := net.JoinHostPort("", "8080")
	logger.Info("listening on address", zap.String("listen_address", address))
	if err := http.ListenAndServe(address, nil); err != nil {
		logger.Error("error starting HTTP server", zap.Error(err))
		os.Exit(1)
	}
}
