package main

import (
	"context"
	"fmt"
	"github.com/cloudhut/kminion/v2/e2e"
	"github.com/cloudhut/kminion/v2/kafka"
	"github.com/cloudhut/kminion/v2/logging"
	"github.com/cloudhut/kminion/v2/minion"
	"github.com/cloudhut/kminion/v2/prometheus"
	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
)

var (
	// ------------------------------------------------------------------------
	// Below parameters are set at build time using ldflags.
	// ------------------------------------------------------------------------

	// version is KMinion's SemVer version (for example: v1.0.0).
	version = "development"
	// builtAt is a string that represent a human-readable date when the binary was built.
	builtAt = "N/A"
	// commit is a string that represents the last git commit for this build.
	commit = "N/A"
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
	logger := logging.NewLogger(cfg.Logger, cfg.Exporter.Namespace).Named("main")
	logger.Info("started kminion", zap.String("version", version), zap.String("built_at", builtAt))

	// set GOMAXPROCS automatically
	l := func(format string, a ...interface{}) {
		logger.Info(fmt.Sprintf(format, a...))
	}
	if _, err = maxprocs.Set(maxprocs.Logger(l)); err != nil {
		logger.Fatal("failed to set GOMAXPROCS automatically", zap.Error(err))
	}

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

	wrappedRegisterer := promclient.WrapRegistererWithPrefix(cfg.Exporter.Namespace+"_", promclient.DefaultRegisterer)

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
			ctx,
			cfg.Minion.EndToEnd,
			logger,
			kafkaSvc,
			wrappedRegisterer,
		)
		if err != nil {
			logger.Fatal("failed to create end-to-end monitoring service: %w", zap.Error(err))
		}

		if err = e2eService.Start(ctx); err != nil {
			logger.Fatal("failed to start end-to-end monitoring service", zap.Error(err))
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
