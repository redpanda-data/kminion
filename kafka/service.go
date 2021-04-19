package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"go.uber.org/zap"
)

type Service struct {
	cfg    Config
	Client *kgo.Client
	logger *zap.Logger
}

func NewService(cfg Config, logger *zap.Logger, opts []kgo.Opt) (*Service, error) {
	// Create Kafka Client
	hooksChildLogger := logger.With(zap.String("source", "kafka_client_hooks"))
	clientHooks := newClientHooks(hooksChildLogger, "")

	kgoOpts, err := NewKgoConfig(cfg, logger, clientHooks)
	for _, opt := range opts {
		kgoOpts = append(kgoOpts, opt)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create a valid kafka Client config: %w", err)
	}

	kafkaClient, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka Client: %w", err)
	}

	return &Service{
		cfg:    cfg,
		Client: kafkaClient,
		logger: logger,
	}, nil
}

// TestConnection tries to fetch Broker metadata and prints some information if connection succeeds. An error will be
// returned if connecting fails.
func (s *Service) TestConnection(ctx context.Context) error {
	s.logger.Info("connecting to Kafka seed brokers, trying to fetch cluster metadata",
		zap.String("seed_brokers", strings.Join(s.cfg.Brokers, ",")))

	req := kmsg.MetadataRequest{
		Topics: nil,
	}
	res, err := req.RequestWith(ctx, s.Client)
	if err != nil {
		return fmt.Errorf("failed to request metadata: %w", err)
	}

	// Request versions in order to guess Kafka Cluster version
	versionsReq := kmsg.NewApiVersionsRequest()
	versionsRes, err := versionsReq.RequestWith(ctx, s.Client)
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
