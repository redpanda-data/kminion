package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"go.uber.org/zap"
)

type Service struct {
	cfg    Config
	logger *zap.Logger
}

func NewService(cfg Config, logger *zap.Logger) *Service {
	return &Service{
		cfg:    cfg,
		logger: logger.Named("kafka_service"),
	}
}

// StartConnectionHealthCheck starts the opt-in per-broker connection health
// check. It returns immediately; the check runs in a background goroutine
// until ctx is canceled. It is a no-op if enabled is false, so existing
// deployments that don't set this config key see no behavior change.
//
// The config type this feature is described by (minion.ConnectionHealthCheckConfig)
// lives outside this package to keep it nested under the minion: config
// block; this method takes the two values out of it directly rather than
// the struct itself, so this package has no dependency on minion's types
// (which would otherwise be an import cycle, since minion already imports
// kafka).
//
// The check is fully independent of the client this Service (and the minion
// and end-to-end services) use for their own checks: it creates its own
// throwaway client every tick and never touches theirs.
func (s *Service) StartConnectionHealthCheck(ctx context.Context, enabled bool, probeInterval time.Duration, promRegisterer prometheus.Registerer) {
	if !enabled {
		return
	}

	logger := s.logger.Named("connection_health_check")
	metrics := newConnectionProbeMetrics(promRegisterer)
	newProber := func(ctx context.Context) (brokerConnectionProber, error) {
		return newLiveBrokerProber(ctx, s.cfg, logger)
	}

	go runConnectionHealthCheck(ctx, newProber, probeInterval, metrics, logger)
}

// CreateAndTestClient creates a client with the services default settings
// logger: will be used to log connections, errors, warnings about tls config, ...
func (s *Service) CreateAndTestClient(ctx context.Context, l *zap.Logger, opts []kgo.Opt) (*kgo.Client, error) {
	logger := l.Named("kgo_client")
	// Config with default options
	kgoOpts, err := NewKgoConfig(s.cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create a valid kafka Client config: %w", err)
	}
	// Append user (the service calling this method) provided options
	kgoOpts = append(kgoOpts, opts...)

	// Create kafka client
	client, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka Client: %w", err)
	}

	// Test connection
	for {
		err = s.testConnection(client, ctx)
		if err == nil {
			break
		}

		if !s.cfg.RetryInitConnection {
			return nil, fmt.Errorf("failed to test connectivity to Kafka cluster %w", err)
		}

		logger.Warn("failed to test connectivity to Kafka cluster, retrying in 5 seconds", zap.Error(err))
		time.Sleep(time.Second * 5)
	}

	return client, nil
}

// Brokers returns list of brokers this service is connecting to
func (s *Service) Brokers() []string {
	return s.cfg.Brokers
}

// testConnection tries to fetch Broker metadata and prints some information if connection succeeds. An error will be
// returned if connecting fails.
func (s *Service) testConnection(client *kgo.Client, ctx context.Context) error {
	connectCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	req := kmsg.MetadataRequest{
		Topics: nil,
	}
	res, err := req.RequestWith(connectCtx, client)
	if err != nil {
		return fmt.Errorf("failed to request metadata: %w", err)
	}

	// Request versions in order to guess Kafka Cluster version
	versionsReq := kmsg.NewApiVersionsRequest()
	versionsRes, err := versionsReq.RequestWith(connectCtx, client)
	if err != nil {
		return fmt.Errorf("failed to request api versions: %w", err)
	}
	err = kerr.ErrorForCode(versionsRes.ErrorCode)
	if err != nil {
		return fmt.Errorf("failed to request api versions. Inner kafka error: %w", err)
	}
	versions := kversion.FromApiVersionsResponse(versionsRes)

	s.logger.Debug("successfully connected to kafka cluster",
		zap.Int("advertised_broker_count", len(res.Brokers)),
		zap.Int("topic_count", len(res.Topics)),
		zap.Int32("controller_id", res.ControllerID),
		zap.String("kafka_version", versions.VersionGuess()))

	return nil
}
