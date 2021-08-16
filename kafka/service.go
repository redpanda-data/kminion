package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

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
	connectCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	err = s.testConnection(client, connectCtx)
	if err != nil {
		logger.Fatal("failed to test connectivity to Kafka cluster", zap.Error(err))
	}

	return client, nil
}

// testConnection tries to fetch Broker metadata and prints some information if connection succeeds. An error will be
// returned if connecting fails.
func (s *Service) testConnection(client *kgo.Client, ctx context.Context) error {
	s.logger.Info("connecting to Kafka seed brokers, trying to fetch cluster metadata",
		zap.String("seed_brokers", strings.Join(s.cfg.Brokers, ",")))

	req := kmsg.MetadataRequest{
		Topics: nil,
	}
	res, err := req.RequestWith(ctx, client)
	if err != nil {
		return fmt.Errorf("failed to request metadata: %w", err)
	}

	// Request versions in order to guess Kafka Cluster version
	versionsReq := kmsg.NewApiVersionsRequest()
	versionsRes, err := versionsReq.RequestWith(ctx, client)
	if err != nil {
		return fmt.Errorf("failed to request api versions: %w", err)
	}
	err = kerr.ErrorForCode(versionsRes.ErrorCode)
	if err != nil {
		return fmt.Errorf("failed to request api versions. Inner kafka error: %w", err)
	}
	versions := kversion.FromApiVersionsResponse(versionsRes)

	s.logger.Info("successfully connected to kafka cluster",
		zap.Int("advertised_broker_count", len(res.Brokers)),
		zap.Int("topic_count", len(res.Topics)),
		zap.Int32("controller_id", res.ControllerID),
		zap.String("kafka_version", versions.VersionGuess()))

	return nil
}
