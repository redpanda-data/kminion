package e2e

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/cloudhut/kminion/v2/kafka"
)

type Service struct {
	// General
	config Config
	logger *zap.Logger

	kafkaSvc *kafka.Service // creates kafka client for us
	client   *kgo.Client

	// Service
	minionID       string          // unique identifier, reported in metrics, in case multiple instances run at the same time
	groupId        string          // our own consumer group
	groupTracker   *groupTracker   // tracks consumer groups starting with the kminion prefix and deletes them if they are unused for some time
	messageTracker *messageTracker // tracks successfully produced messages,
	clientHooks    *clientHooks    // logs broker events, tracks the coordinator (i.e. which broker last responded to our offset commit)
	partitionCount int             // number of partitions of our test topic, used to send messages to all partitions

	// Metrics
	messagesProducedInFlight *prometheus.GaugeVec
	messagesProducedTotal    *prometheus.CounterVec
	messagesProducedFailed   *prometheus.CounterVec
	messagesReceived         *prometheus.CounterVec
	offsetCommitsTotal       *prometheus.CounterVec
	offsetCommitsFailedTotal *prometheus.CounterVec
	lostMessages             *prometheus.CounterVec

	produceLatency      *prometheus.HistogramVec
	roundtripLatency    *prometheus.HistogramVec
	offsetCommitLatency *prometheus.HistogramVec
}

// NewService creates a new instance of the e2e monitoring service (wow)
func NewService(ctx context.Context, cfg Config, logger *zap.Logger, kafkaSvc *kafka.Service, promRegisterer prometheus.Registerer) (*Service, error) {
	minionID := uuid.NewString()
	groupID := fmt.Sprintf("%v-%v", cfg.Consumer.GroupIdPrefix, minionID)

	// Producer options
	kgoOpts := []kgo.Opt{
		kgo.ProduceRequestTimeout(3 * time.Second),
		kgo.RecordRetries(3),
		// We use the manual partitioner so that the records' partition id will be used as target partition
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	}
	if cfg.Producer.RequiredAcks == "all" {
		kgoOpts = append(kgoOpts, kgo.RequiredAcks(kgo.AllISRAcks()))
	} else {
		kgoOpts = append(kgoOpts, kgo.RequiredAcks(kgo.LeaderAck()))
		kgoOpts = append(kgoOpts, kgo.DisableIdempotentWrite())
	}

	// Consumer configs
	kgoOpts = append(kgoOpts,
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(cfg.TopicManagement.Name),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)

	// Prepare hooks
	hooks := newEndToEndClientHooks(logger)
	kgoOpts = append(kgoOpts, kgo.WithHooks(hooks))

	// Create kafka service and check if client can successfully connect to Kafka cluster
	logger.Info("connecting to Kafka seed brokers, trying to fetch cluster metadata",
		zap.String("seed_brokers", strings.Join(kafkaSvc.Brokers(), ",")))
	client, err := kafkaSvc.CreateAndTestClient(ctx, logger, kgoOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client for e2e: %w", err)
	}
	logger.Info("successfully connected to kafka cluster")

	svc := &Service{
		config:   cfg,
		logger:   logger.Named("e2e"),
		kafkaSvc: kafkaSvc,
		client:   client,

		minionID:    minionID,
		groupId:     groupID,
		clientHooks: hooks,
	}

	svc.groupTracker = newGroupTracker(cfg, logger, client, groupID)
	svc.messageTracker = newMessageTracker(svc)

	makeCounterVec := func(name string, labelNames []string, help string) *prometheus.CounterVec {
		cv := prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: "end_to_end",
			Name:      name,
			Help:      help,
		}, labelNames)
		promRegisterer.MustRegister(cv)
		return cv
	}
	makeGaugeVec := func(name string, labelNames []string, help string) *prometheus.GaugeVec {
		gv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Subsystem: "end_to_end",
			Name:      name,
			Help:      help,
		}, labelNames)
		promRegisterer.MustRegister(gv)
		return gv
	}
	makeHistogramVec := func(name string, maxLatency time.Duration, labelNames []string, help string) *prometheus.HistogramVec {
		hv := prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Subsystem: "end_to_end",
			Name:      name,
			Help:      help,
			Buckets:   createHistogramBuckets(maxLatency),
		}, labelNames)
		promRegisterer.MustRegister(hv)
		return hv
	}

	// Low-level info
	// Users can construct alerts like "can't produce messages" themselves from those
	svc.messagesProducedInFlight = makeGaugeVec("messages_produced_in_flight", []string{"partition_id"}, "Number of messages that kminion's end-to-end test produced but has not received an answer for yet")
	svc.messagesProducedTotal = makeCounterVec("messages_produced_total", []string{"partition_id"}, "Number of all messages produced to Kafka. This counter will be incremented when we receive a response (failure/timeout or success) from Kafka")
	svc.messagesProducedFailed = makeCounterVec("messages_produced_failed_total", []string{"partition_id"}, "Number of messages failed to produce to Kafka because of a timeout or failure")
	svc.messagesReceived = makeCounterVec("messages_received_total", []string{"partition_id"}, "Number of *matching* messages kminion received. Every roundtrip message has a minionID (randomly generated on startup) and a timestamp. Kminion only considers a message a match if it it arrives within the configured roundtrip SLA (and it matches the minionID)")
	svc.offsetCommitsTotal = makeCounterVec("offset_commits_total", []string{"coordinator_id"}, "Counts how many times kminions end-to-end test has committed offsets")
	svc.offsetCommitsFailedTotal = makeCounterVec("offset_commits_failed_total", []string{"coordinator_id", "reason"}, "Number of offset commits that returned an error or timed out")
	svc.lostMessages = makeCounterVec("messages_lost_total", []string{"partition_id"}, "Number of messages that have been produced successfully but not received within the configured SLA duration")

	// Latency Histograms
	// More detailed info about how long stuff took
	// Since histograms also have an 'infinite' bucket, they can be used to detect small hickups "lost" messages
	svc.produceLatency = makeHistogramVec("produce_latency_seconds", cfg.Producer.AckSla, []string{"partition_id"}, "Time until we received an ack for a produced message")
	svc.roundtripLatency = makeHistogramVec("roundtrip_latency_seconds", cfg.Consumer.RoundtripSla, []string{"partition_id"}, "Time it took between sending (producing) and receiving (consuming) a message")
	svc.offsetCommitLatency = makeHistogramVec("offset_commit_latency_seconds", cfg.Consumer.CommitSla, []string{"coordinator_id"}, "Time kafka took to respond to kminion's offset commit")

	return svc, nil
}

// Start starts the service (wow)
func (s *Service) Start(ctx context.Context) error {
	// Ensure topic exists and is configured correctly
	if err := s.validateManagementTopic(ctx); err != nil {
		return fmt.Errorf("could not validate end-to-end topic: %w", err)
	}

	// finally start everything else (producing, consuming, continuous validation, consumer group tracking)
	go s.startReconciliation(ctx)

	// Start consumer and wait until we've received a response for the first poll
	// which would indicate that the consumer is ready. Only if the consumer is
	// ready we want to start the e2e producer to ensure that we will not miss
	// messages because the consumer wasn't ready. However, if this initialization
	// does not succeed within 30s we have to assume, that something is wrong on the
	// consuming or producing side. KMinion is supposed to report these kind of
	// issues and therefore this should not block KMinion from starting.
	initCh := make(chan bool, 1)
	s.logger.Info("initializing consumer and waiting until it has received the first record batch")
	go s.startConsumeMessages(ctx, initCh)

	// Produce an init message until the consumer received at least one fetch
	initTicker := time.NewTicker(1 * time.Second)
	isInitialized := false

	// We send a first message immediately, but we'll keep sending more messages later
	// since the consumers start at the latest offset and may have missed this message.
	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	s.sendInitMessage(initCtx, s.client, s.config.TopicManagement.Name)

	for !isInitialized {
		select {
		case <-initTicker.C:
			s.sendInitMessage(initCtx, s.client, s.config.TopicManagement.Name)
		case <-initCh:
			isInitialized = true
			s.logger.Info("consumer has been successfully initialized")
		case <-initCtx.Done():
			// At this point we just assume the consumers are running fine.
			// The entire cluster may be down or producing fails.
			s.logger.Warn("initializing the consumers timed out, proceeding with the startup")
			isInitialized = true
		case <-ctx.Done():
			return nil
		}
	}
	go s.startOffsetCommits(ctx)
	go s.startProducer(ctx)

	// keep track of groups, delete old unused groups
	if s.config.Consumer.DeleteStaleConsumerGroups {
		go s.groupTracker.start(ctx)
	}

	return nil
}

func (s *Service) sendInitMessage(ctx context.Context, client *kgo.Client, topicName string) {
	// Try to produce one record into each partition. This is important because
	// one or more partitions may be offline, while others may still be writable.
	for i := 0; i < s.partitionCount; i++ {
		client.TryProduce(ctx, &kgo.Record{
			Key:       []byte("init-message"),
			Value:     nil,
			Topic:     topicName,
			Partition: int32(i),
		}, nil)
	}
}

func (s *Service) startReconciliation(ctx context.Context) {
	if !s.config.TopicManagement.Enabled {
		return
	}

	validateTopicTicker := time.NewTicker(s.config.TopicManagement.ReconciliationInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-validateTopicTicker.C:
			err := s.validateManagementTopic(ctx)
			if err != nil {
				s.logger.Error("failed to validate end-to-end topic", zap.Error(err))
			}
		}
	}
}

func (s *Service) startProducer(ctx context.Context) {
	produceTicker := time.NewTicker(s.config.ProbeInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-produceTicker.C:
			s.produceMessagesToAllPartitions(ctx)
		}
	}
}

func (s *Service) startOffsetCommits(ctx context.Context) {
	commitTicker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-commitTicker.C:
			s.commitOffsets(ctx)
		}
	}
}
