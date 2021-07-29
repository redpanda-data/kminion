package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"

	"github.com/cloudhut/kminion/v2/e2e"
	"github.com/cloudhut/kminion/v2/kafka"
	"github.com/cloudhut/kminion/v2/logging"
	"github.com/cloudhut/kminion/v2/minion"
	"github.com/cloudhut/kminion/v2/prometheus"
	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func main() {
	startupLogger, err := zap.NewProduction()
	if err != nil {
		panic(fmt.Errorf("failed to create startup logger: %w", err))
	}

	cfg, err := newConfig(startupLogger)
	if err != nil {
		startupLogger.Fatal("failed to parse config", zap.Error(err))
	}

	logger := logging.NewLogger(cfg.Logger, cfg.Exporter.Namespace)
	if err != nil {
		startupLogger.Fatal("failed to create new logger", zap.Error(err))
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

	// Create kafka service
	kafkaSvc := kafka.NewService(cfg.Kafka, logger)

	// Create minion service
	// Prometheus exporter only talks to the minion service which
	// issues all the requests to Kafka and wraps the interface accordingly.
	minionSvc, err := minion.NewService(cfg.Minion, logger, kafkaSvc, cfg.Exporter.Namespace, ctx)
	if err != nil {
		logger.Fatal("failed to setup minion service", zap.Error(err))
	}

	err = minionSvc.Start(ctx)
	if err != nil {
		logger.Fatal("failed to start minion service", zap.Error(err))
	}

	// Create end to end testing service
	if cfg.Minion.EndToEnd.Enabled {
		e2eService, err := e2e.NewService(
			cfg.Minion.EndToEnd,
			logger,
			kafkaSvc,
			cfg.Exporter.Namespace,
			ctx,
		)
		if err != nil {
			logger.Fatal("failed to create end-to-end monitoring service: %w", zap.Error(err))
		}

		if err = e2eService.Start(ctx); err != nil {
			logger.Fatal("failed to start end-to-end monitoring service: %w", zap.Error(err))
		}
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
	http.Handle("/ready", minionSvc.HandleIsReady())

	// Start HTTP server
	address := net.JoinHostPort(cfg.Exporter.Host, strconv.Itoa(cfg.Exporter.Port))
	logger.Info("listening on address", zap.String("listen_address", address))
	if err := http.ListenAndServe(address, nil); err != nil {
		logger.Error("error starting HTTP server", zap.Error(err))
		os.Exit(1)
	}
}
