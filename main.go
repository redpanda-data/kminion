package main

import (
	"context"
	"errors"
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
	if err != nil {
		startupLogger.Fatal("failed to create new logger", zap.Error(err))
	}

	logger.Info("started kminion", zap.String("version", version), zap.String("built_at", builtAt))

	// Setup context that stops when the application receives an interrupt signal
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

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
	srv := &http.Server{Addr: address}
	go func() {
		<-ctx.Done()
		if err := srv.Shutdown(context.Background()); err != nil {
			logger.Error("error stopping HTTP server", zap.Error(err))
			os.Exit(1)
		}
	}()
	logger.Info("listening on address", zap.String("listen_address", address))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("error starting HTTP server", zap.Error(err))
		os.Exit(1)
	}

	logger.Info("kminion stopped")
}
