package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/exporter-toolkit/web"
	"github.com/redpanda-data/kminion/v2/e2e"
	"github.com/redpanda-data/kminion/v2/kafka"
	"github.com/redpanda-data/kminion/v2/logging"
	"github.com/redpanda-data/kminion/v2/minion"
	"github.com/redpanda-data/kminion/v2/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/exp/zapslog"
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

const (
	// metricsHandlerTimeout gives a collection time to finish before the handler stops it.
	metricsHandlerTimeout = prometheus.CollectTimeout + 10*time.Second
	// metricsWriteTimeout gives the handler time to finish and then time to send the buffered response.
	metricsWriteTimeout = metricsHandlerTimeout + 20*time.Second
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

	// Setup context that stops when the application receives an interrupt or termination signal
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	wrappedRegisterer := promclient.WrapRegistererWithPrefix(cfg.Exporter.Namespace+"_", promclient.DefaultRegisterer)

	// Create kafka service
	kafkaSvc := kafka.NewService(cfg.Kafka, logger)

	// Start the opt-in per-broker connection health check. This is a no-op
	// unless cfg.Minion.ConnectionHealthCheck.Enabled is set, and is
	// independent of the minion and end-to-end services started below: it
	// never shares a client with either.
	kafkaSvc.StartConnectionHealthCheck(ctx, cfg.Minion.ConnectionHealthCheck.Enabled, cfg.Minion.ConnectionHealthCheck.ProbeInterval, wrappedRegisterer)

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
				// Every scrape sends requests to all brokers, so limit the parallel scrapes. 3 allows an HA Prometheus
				// pair and one more client. Other requests get a 503 at once.
				// With a Timeout, the handler writes the response to a buffer and frees its slot before the response
				// goes to the client, so a client that does not read cannot hold a slot.
				promhttp.HandlerOpts{MaxRequestsInFlight: 3, Timeout: metricsHandlerTimeout},
			),
		),
	)
	http.Handle("/ready", minionSvc.HandleIsReady())

	// Start HTTP server
	address := net.JoinHostPort(cfg.Exporter.Host, strconv.Itoa(cfg.Exporter.Port))
	// WriteTimeout closes the connection of a client that does not read the response, so the buffered response
	// does not stay in memory.
	// IdleTimeout closes keep-alive connections that wait for a new request. It is longer than common scrape intervals.
	srv := &http.Server{Addr: address, ReadHeaderTimeout: 10 * time.Second, WriteTimeout: metricsWriteTimeout, IdleTimeout: 2 * time.Minute}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		logger.Fatal("failed to listen on address", zap.String("listen_address", address), zap.Error(err))
	}
	go func() {
		<-ctx.Done()
		if err := srv.Shutdown(context.Background()); err != nil {
			logger.Error("error stopping HTTP server", zap.Error(err))
			os.Exit(1)
		}
	}()
	logger.Info("listening on address", zap.String("listen_address", address), zap.Bool("web_config", cfg.Exporter.WebConfigFilepath != ""))
	if err := serveHTTP(listener, srv, cfg.Exporter.WebConfigFilepath, logger); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("error starting HTTP server", zap.Error(err))
		os.Exit(1)
	}

	logger.Info("kminion stopped")
}

// serveHTTP serves srv on l. With a web config file, the TLS settings of that file apply.
func serveHTTP(l net.Listener, srv *http.Server, webConfigFilepath string, logger *zap.Logger) error {
	// Send the server errors, for example failed TLS handshakes, to the JSON log.
	errorLog, err := zap.NewStdLogAt(logger.Named("http"), zap.WarnLevel)
	if err != nil {
		return fmt.Errorf("failed to create the HTTP server error log: %w", err)
	}
	srv.ErrorLog = errorLog

	if webConfigFilepath == "" {
		return srv.Serve(l)
	}
	slogger := slog.New(zapslog.NewHandler(logger.Core(), zapslog.WithName("main")))
	return web.Serve(l, srv, &web.FlagConfig{WebConfigFile: &webConfigFilepath}, slogger)
}
